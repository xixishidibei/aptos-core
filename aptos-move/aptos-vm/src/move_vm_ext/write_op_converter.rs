// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::move_vm_ext::{session::BytesWithResourceLayout, AptosMoveResolver};
use aptos_aggregator::delta_change_set::serialize;
use aptos_types::{
    on_chain_config::{CurrentTimeMicroseconds, OnChainConfig},
    state_store::{state_key::StateKey, state_value::StateValueMetadata},
    write_set::WriteOp,
};
use aptos_vm_types::{
    abstract_write_op::GroupWrite, module_and_script_storage::module_storage::AptosModuleStorage,
    resolver::ResourceGroupSize, resource_group_adapter::group_tagged_resource_size,
};
use bytes::Bytes;
use move_binary_format::{
    errors::{PartialVMError, PartialVMResult},
    CompiledModule,
};
use move_core_types::{
    effects::{Op as MoveStorageOp, Op},
    language_storage::StructTag,
    value::MoveTypeLayout,
    vm_status::StatusCode,
};
use move_vm_types::delayed_values::error::code_invariant_error;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct WriteOpConverter<'r> {
    remote: &'r dyn AptosMoveResolver,
    new_slot_metadata: Option<StateValueMetadata>,
}

macro_rules! convert_impl {
    ($convert_func_name:ident, $get_metadata_callback:ident) => {
        pub(crate) fn $convert_func_name(
            &self,
            state_key: &StateKey,
            move_storage_op: MoveStorageOp<Bytes>,
            legacy_creation_as_modification: bool,
        ) -> PartialVMResult<WriteOp> {
            let state_value_metadata = self
                .remote
                .as_executor_view()
                .$get_metadata_callback(state_key)?;
            self.convert(
                state_value_metadata,
                move_storage_op,
                legacy_creation_as_modification,
            )
        }
    };
}

// We set SPECULATIVE_EXECUTION_ABORT_ERROR here, as the error can happen due to
// speculative reads (and in a non-speculative context, e.g. during commit, it
// is a more serious error and block execution must abort).
// BlockExecutor is responsible with handling this error.
fn group_size_arithmetics_error() -> PartialVMError {
    PartialVMError::new(StatusCode::SPECULATIVE_EXECUTION_ABORT_ERROR)
        .with_message("Group size arithmetics error while applying updates".to_string())
}

fn decrement_size_for_remove_tag(
    size: &mut ResourceGroupSize,
    old_tagged_resource_size: u64,
) -> PartialVMResult<()> {
    match size {
        ResourceGroupSize::Concrete(_) => Err(code_invariant_error(
            "Unexpected ResourceGroupSize::Concrete in decrement_size_for_remove_tag",
        )),
        ResourceGroupSize::Combined {
            num_tagged_resources,
            all_tagged_resources_size,
        } => {
            *num_tagged_resources = num_tagged_resources
                .checked_sub(1)
                .ok_or_else(group_size_arithmetics_error)?;
            *all_tagged_resources_size = all_tagged_resources_size
                .checked_sub(old_tagged_resource_size)
                .ok_or_else(group_size_arithmetics_error)?;
            Ok(())
        },
    }
}

fn increment_size_for_add_tag(
    size: &mut ResourceGroupSize,
    new_tagged_resource_size: u64,
) -> PartialVMResult<()> {
    match size {
        ResourceGroupSize::Concrete(_) => Err(PartialVMError::new(
            StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
        )
        .with_message(
            "Unexpected ResourceGroupSize::Concrete in increment_size_for_add_tag".to_string(),
        )),
        ResourceGroupSize::Combined {
            num_tagged_resources,
            all_tagged_resources_size,
        } => {
            *num_tagged_resources = num_tagged_resources
                .checked_add(1)
                .ok_or_else(group_size_arithmetics_error)?;
            *all_tagged_resources_size = all_tagged_resources_size
                .checked_add(new_tagged_resource_size)
                .ok_or_else(group_size_arithmetics_error)?;
            Ok(())
        },
    }
}

fn check_size_and_existence_match(
    size: &ResourceGroupSize,
    exists: bool,
    state_key: &StateKey,
) -> PartialVMResult<()> {
    if exists {
        if size.get() == 0 {
            Err(
                PartialVMError::new(StatusCode::SPECULATIVE_EXECUTION_ABORT_ERROR).with_message(
                    format!(
                        "Group tag count/size shouldn't be 0 for an existing group: {:?}",
                        state_key
                    ),
                ),
            )
        } else {
            Ok(())
        }
    } else if size.get() > 0 {
        Err(
            PartialVMError::new(StatusCode::SPECULATIVE_EXECUTION_ABORT_ERROR).with_message(
                format!(
                    "Group tag count/size should be 0 for a new group: {:?}",
                    state_key
                ),
            ),
        )
    } else {
        Ok(())
    }
}

impl<'r> WriteOpConverter<'r> {
    convert_impl!(convert_module, get_module_state_value_metadata);

    convert_impl!(convert_aggregator, get_aggregator_v1_state_value_metadata);

    pub(crate) fn new(
        remote: &'r dyn AptosMoveResolver,
        is_storage_slot_metadata_enabled: bool,
    ) -> Self {
        let mut new_slot_metadata: Option<StateValueMetadata> = None;
        if is_storage_slot_metadata_enabled {
            if let Some(current_time) = CurrentTimeMicroseconds::fetch_config(remote) {
                // The deposit on the metadata is a placeholder (0), it will be updated later when
                // storage fee is charged.
                new_slot_metadata = Some(StateValueMetadata::placeholder(&current_time));
            }
        }

        Self {
            remote,
            new_slot_metadata,
        }
    }

    /// Converts module bytes and their compiled representation extracted from publish
    /// request into write ops. Only used by V2 loader implementation.
    pub(crate) fn convert_modules_into_write_ops(
        &self,
        module_storage: &impl AptosModuleStorage,
        code_and_modules: impl IntoIterator<Item = (Bytes, CompiledModule)>,
    ) -> PartialVMResult<BTreeMap<StateKey, WriteOp>> {
        let mut write_ops = BTreeMap::new();
        for (bytes, compiled_module) in code_and_modules.into_iter() {
            let addr = compiled_module.self_addr();
            let name = compiled_module.self_name();

            let module_exists = module_storage.check_module_exists(addr, name)?;
            let op = if module_exists {
                Op::Modify(bytes)
            } else {
                Op::New(bytes)
            };

            let state_value_metadata = module_storage.fetch_state_value_metadata(addr, name)?;
            let write_op = self.convert(
                state_value_metadata,
                op,
                // For modules, creation is never a modification.
                false,
            )?;

            let state_key = StateKey::module(addr, name);
            write_ops.insert(state_key, write_op);
        }
        Ok(write_ops)
    }

    pub(crate) fn convert_resource(
        &self,
        state_key: &StateKey,
        move_storage_op: MoveStorageOp<BytesWithResourceLayout>,
        legacy_creation_as_modification: bool,
    ) -> PartialVMResult<(WriteOp, Option<Arc<MoveTypeLayout>>)> {
        let state_value_metadata = self
            .remote
            .as_executor_view()
            .get_resource_state_value_metadata(state_key)?;
        let (move_storage_op, layout) = match move_storage_op {
            MoveStorageOp::New((data, layout)) => (MoveStorageOp::New(data), layout),
            MoveStorageOp::Modify((data, layout)) => (MoveStorageOp::Modify(data), layout),
            MoveStorageOp::Delete => (MoveStorageOp::Delete, None),
        };

        let write_op = self.convert(
            state_value_metadata,
            move_storage_op,
            legacy_creation_as_modification,
        )?;
        Ok((write_op, layout))
    }

    pub(crate) fn convert_resource_group_v1(
        &self,
        state_key: &StateKey,
        group_changes: BTreeMap<StructTag, MoveStorageOp<BytesWithResourceLayout>>,
    ) -> PartialVMResult<GroupWrite> {
        // Resource group metadata is stored at the group StateKey, and can be obtained via the
        // same interfaces at for a resource at a given StateKey.
        let state_value_metadata = self
            .remote
            .as_executor_view()
            .get_resource_state_value_metadata(state_key)?;
        // Currently, due to read-before-write and a gas charge on the first read that is based
        // on the group size, this should simply re-read a cached (speculative) group size.
        let pre_group_size = self.remote.resource_group_size(state_key)?;
        check_size_and_existence_match(&pre_group_size, state_value_metadata.is_some(), state_key)?;

        let mut inner_ops = BTreeMap::new();
        let mut post_group_size = pre_group_size;

        for (tag, current_op) in group_changes {
            // We take speculative group size prior to the transaction, and update it based on the change-set.
            // For each tagged resource in the change set, we subtract the previous size tagged resource size,
            // and then add new tagged resource size.
            //
            // The reason we do not instead get and add the sizes of the resources in the group,
            // but not in the change-set, is to avoid creating unnecessary R/W conflicts (the resources
            // in the change-set are already read, but the other resources are not).
            if !matches!(current_op, MoveStorageOp::New(_)) {
                let old_tagged_value_size = self.remote.resource_size_in_group(state_key, &tag)?;
                let old_size = group_tagged_resource_size(&tag, old_tagged_value_size)?;
                decrement_size_for_remove_tag(&mut post_group_size, old_size)?;
            }

            match &current_op {
                MoveStorageOp::Modify((data, _)) | MoveStorageOp::New((data, _)) => {
                    let new_size = group_tagged_resource_size(&tag, data.len())?;
                    increment_size_for_add_tag(&mut post_group_size, new_size)?;
                },
                MoveStorageOp::Delete => {},
            };

            let legacy_op = match current_op {
                MoveStorageOp::Delete => (WriteOp::legacy_deletion(), None),
                MoveStorageOp::Modify((data, maybe_layout)) => {
                    (WriteOp::legacy_modification(data), maybe_layout)
                },
                MoveStorageOp::New((data, maybe_layout)) => {
                    (WriteOp::legacy_creation(data), maybe_layout)
                },
            };
            inner_ops.insert(tag, legacy_op);
        }

        // Create an op to encode the proper kind for resource group operation.
        let metadata_op = if post_group_size.get() == 0 {
            MoveStorageOp::Delete
        } else if pre_group_size.get() == 0 {
            MoveStorageOp::New(Bytes::new())
        } else {
            MoveStorageOp::Modify(Bytes::new())
        };
        Ok(GroupWrite::new(
            self.convert(state_value_metadata, metadata_op, false)?,
            inner_ops,
            post_group_size,
            pre_group_size.get(),
        ))
    }

    fn convert(
        &self,
        state_value_metadata: Option<StateValueMetadata>,
        move_storage_op: MoveStorageOp<Bytes>,
        legacy_creation_as_modification: bool,
    ) -> PartialVMResult<WriteOp> {
        use MoveStorageOp::*;
        use WriteOp::*;
        let write_op = match (state_value_metadata, move_storage_op) {
            (None, Modify(_) | Delete) => {
                // Possible under speculative execution, returning speculative error waiting for re-execution.
                return Err(
                    PartialVMError::new(StatusCode::SPECULATIVE_EXECUTION_ABORT_ERROR)
                        .with_message(
                            "When converting write op: updating non-existent value.".to_string(),
                        ),
                );
            },
            (Some(_), New(_)) => {
                // Possible under speculative execution, returning speculative error waiting for re-execution.
                return Err(
                    PartialVMError::new(StatusCode::SPECULATIVE_EXECUTION_ABORT_ERROR)
                        .with_message(
                            "When converting write op: Recreating existing value.".to_string(),
                        ),
                );
            },
            (None, New(data)) => match &self.new_slot_metadata {
                None => {
                    if legacy_creation_as_modification {
                        WriteOp::legacy_modification(data)
                    } else {
                        WriteOp::legacy_creation(data)
                    }
                },
                Some(metadata) => Creation {
                    data,
                    metadata: metadata.clone(),
                },
            },
            (Some(metadata), Modify(data)) => {
                // Inherit metadata even if the feature flags is turned off, for compatibility.
                Modification { data, metadata }
            },
            (Some(metadata), Delete) => {
                // Inherit metadata even if the feature flags is turned off, for compatibility.
                Deletion { metadata }
            },
        };
        Ok(write_op)
    }

    pub(crate) fn convert_aggregator_modification(
        &self,
        state_key: &StateKey,
        value: u128,
    ) -> PartialVMResult<WriteOp> {
        let maybe_existing_metadata = self
            .remote
            .get_aggregator_v1_state_value_metadata(state_key)?;
        let data = serialize(&value).into();

        let op = match maybe_existing_metadata {
            None => {
                match &self.new_slot_metadata {
                    // n.b. Aggregator writes historically did not distinguish Create vs Modify.
                    None => WriteOp::legacy_modification(data),
                    Some(metadata) => WriteOp::Creation {
                        data,
                        metadata: metadata.clone(),
                    },
                }
            },
            Some(metadata) => WriteOp::Modification { data, metadata },
        };

        Ok(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_cache::{tests::as_resolver_with_group_size_kind, AsMoveResolver},
        move_vm_ext::resolver::ResourceGroupResolver,
    };
    use aptos_types::{
        account_address::AccountAddress,
        state_store::{
            errors::StateviewError, state_storage_usage::StateStorageUsage,
            state_value::StateValue, TStateView,
        },
        write_set::TransactionWrite,
    };
    use aptos_vm_types::{
        module_and_script_storage::AsAptosCodeStorage,
        resource_group_adapter::{group_size_as_sum, GroupSizeKind},
    };
    use claims::{assert_none, assert_ok, assert_some, assert_some_eq};
    use move_binary_format::file_format::empty_module_with_dependencies_and_friends;
    use move_core_types::{
        identifier::Identifier,
        language_storage::{StructTag, TypeTag},
    };
    use move_vm_runtime::move_vm::MoveVM;
    use once_cell::sync::Lazy;

    fn raw_metadata(v: u64) -> StateValueMetadata {
        StateValueMetadata::legacy(v, &CurrentTimeMicroseconds { microseconds: v })
    }

    // TODO: Can re-use some of these testing definitions with aptos-vm-types.
    pub(crate) fn mock_tag_0() -> StructTag {
        StructTag {
            address: AccountAddress::ONE,
            module: Identifier::new("a").unwrap(),
            name: Identifier::new("a").unwrap(),
            type_args: vec![TypeTag::U8],
        }
    }

    pub(crate) fn mock_tag_1() -> StructTag {
        StructTag {
            address: AccountAddress::ONE,
            module: Identifier::new("abcde").unwrap(),
            name: Identifier::new("fgh").unwrap(),
            type_args: vec![TypeTag::U64],
        }
    }

    pub(crate) fn mock_tag_2() -> StructTag {
        StructTag {
            address: AccountAddress::ONE,
            module: Identifier::new("abcdex").unwrap(),
            name: Identifier::new("fghx").unwrap(),
            type_args: vec![TypeTag::U128],
        }
    }

    struct MockStateView {
        data: BTreeMap<StateKey, StateValue>,
    }

    impl MockStateView {
        fn new(data: BTreeMap<StateKey, StateValue>) -> Self {
            Self { data }
        }
    }

    impl TStateView for MockStateView {
        type Key = StateKey;

        fn get_state_value(
            &self,
            state_key: &Self::Key,
        ) -> Result<Option<StateValue>, StateviewError> {
            Ok(self.data.get(state_key).cloned())
        }

        fn get_usage(&self) -> Result<StateStorageUsage, StateviewError> {
            unimplemented!();
        }
    }

    static MOCK_VM: Lazy<MoveVM> = Lazy::new(|| MoveVM::new(vec![]));

    fn module(name: &str) -> (StateKey, Bytes, CompiledModule) {
        let module = empty_module_with_dependencies_and_friends(name, vec![], vec![]);
        let state_key = StateKey::module(module.self_addr(), module.self_name());
        let mut module_bytes = vec![];
        assert_ok!(module.serialize(&mut module_bytes));
        (state_key, module_bytes.into(), module)
    }

    #[test]
    fn test_convert_modules_into_write_ops() {
        // Create a state value with no metadata.
        let (a_state_key, a_bytes, a) = module("a");
        let a_state_value = StateValue::new_legacy(a_bytes.clone());

        // Create a state value with legacy metadata.
        let (b_state_key, b_bytes, b) = module("b");
        let b_state_value = StateValue::new_with_metadata(
            b_bytes.clone(),
            StateValueMetadata::legacy(10, &CurrentTimeMicroseconds { microseconds: 100 }),
        );

        // Create a state value with non-legacy metadata.
        let (c_state_key, c_bytes, c) = module("c");
        let c_state_value = StateValue::new_with_metadata(
            c_bytes.clone(),
            StateValueMetadata::new(20, 30, &CurrentTimeMicroseconds { microseconds: 200 }),
        );

        // Module that does not yet exist.
        let (d_state_key, d_bytes, d) = module("d");

        // Create the configuration time resource in the state as well;
        let current_time = CurrentTimeMicroseconds { microseconds: 300 };
        let state_key = assert_ok!(StateKey::resource(
            CurrentTimeMicroseconds::address(),
            &CurrentTimeMicroseconds::struct_tag()
        ));
        let bytes = assert_ok!(bcs::to_bytes(&current_time));
        let state_value = StateValue::new_legacy(bytes.into());

        // Setting up the state.
        let state_view = MockStateView::new(BTreeMap::from([
            (state_key, state_value),
            (a_state_key.clone(), a_state_value.clone()),
            (b_state_key.clone(), b_state_value.clone()),
            (c_state_key.clone(), c_state_value.clone()),
        ]));
        let resolver = state_view.as_move_resolver();
        let code_storage = state_view.as_aptos_code_storage(&MOCK_VM);
        // Storage slot metadata is enabled on the mainnet.
        let woc = WriteOpConverter::new(&resolver, true);

        let modules = vec![
            (a_bytes.clone(), a),
            (b_bytes.clone(), b),
            (c_bytes.clone(), c),
            (d_bytes.clone(), d),
        ];

        let results = assert_ok!(woc.convert_modules_into_write_ops(&code_storage, modules));
        assert_eq!(results.len(), 4);

        // For `a`, `b`, and `c`, since they exist, metadata is inherited
        // the write op is a creation.

        let a_write_op = assert_some!(results.get(&a_state_key));
        assert!(a_write_op.is_modification());
        assert_eq!(assert_some!(a_write_op.bytes()), &a_bytes);
        assert_eq!(a_write_op.metadata(), &a_state_value.into_metadata());

        let b_write_op = assert_some!(results.get(&b_state_key));
        assert!(b_write_op.is_modification());
        assert_eq!(assert_some!(b_write_op.bytes()), &b_bytes);
        assert_eq!(b_write_op.metadata(), &b_state_value.into_metadata());

        let c_write_op = assert_some!(results.get(&c_state_key));
        assert!(c_write_op.is_modification());
        assert_eq!(assert_some!(c_write_op.bytes()), &c_bytes);
        assert_eq!(c_write_op.metadata(), &c_state_value.into_metadata());

        // Since `d` does not exist, its metadata is a placeholder.
        let d_write_op = assert_some!(results.get(&d_state_key));
        assert!(d_write_op.is_creation());
        assert_eq!(assert_some!(d_write_op.bytes()), &d_bytes);
        assert_eq!(
            d_write_op.metadata(),
            &StateValueMetadata::placeholder(&current_time)
        )
    }

    // TODO[agg_v2](test) make as_resolver_with_group_size_kind support AsSum
    // #[test]
    #[allow(unused)]
    fn size_computation_delete_modify_ops() {
        let group: BTreeMap<StructTag, Bytes> = BTreeMap::from([
            (mock_tag_0(), vec![1].into()),
            (mock_tag_1(), vec![2, 2].into()),
            (mock_tag_2(), vec![3, 3, 3].into()),
        ]);
        let metadata = raw_metadata(100);
        let key = StateKey::raw(&[0]);

        let data = BTreeMap::from([(
            key.clone(),
            StateValue::new_with_metadata(bcs::to_bytes(&group).unwrap().into(), metadata.clone()),
        )]);

        let expected_size = group_size_as_sum(
            vec![(&mock_tag_0(), 1), (&mock_tag_1(), 2), (&mock_tag_2(), 3)].into_iter(),
        )
        .unwrap();

        let s = MockStateView::new(data);
        let resolver = as_resolver_with_group_size_kind(&s, GroupSizeKind::AsSum);

        assert_eq!(resolver.resource_group_size(&key).unwrap(), expected_size);
        // TODO[agg_v2](test): Layout hardcoded to None. Test with layout = Some(..)
        let group_changes = BTreeMap::from([
            (mock_tag_0(), MoveStorageOp::Delete),
            (
                mock_tag_2(),
                MoveStorageOp::Modify((vec![5, 5, 5, 5, 5].into(), None)),
            ),
        ]);
        let converter = WriteOpConverter::new(&resolver, false);
        let group_write = converter
            .convert_resource_group_v1(&key, group_changes)
            .unwrap();

        assert_eq!(group_write.metadata_op().metadata(), &metadata);
        let expected_new_size =
            group_size_as_sum(vec![(&mock_tag_1(), 2), (&mock_tag_2(), 5)].into_iter()).unwrap();
        assert_some_eq!(group_write.maybe_group_op_size(), expected_new_size);
        assert_eq!(group_write.inner_ops().len(), 2);
        assert_some_eq!(
            group_write.inner_ops().get(&mock_tag_0()),
            &(WriteOp::legacy_deletion(), None)
        );
        assert_some_eq!(
            group_write.inner_ops().get(&mock_tag_2()),
            &(
                WriteOp::legacy_modification(vec![5, 5, 5, 5, 5].into()),
                None
            )
        );
    }

    // TODO[agg_v2](test) make as_resolver_with_group_size_kind support AsSum
    // #[test]
    #[allow(unused)]
    fn size_computation_new_op() {
        let group: BTreeMap<StructTag, Bytes> = BTreeMap::from([
            (mock_tag_0(), vec![1].into()),
            (mock_tag_1(), vec![2, 2].into()),
        ]);
        let metadata = raw_metadata(100);
        let key = StateKey::raw(&[0]);

        let data = BTreeMap::from([(
            key.clone(),
            StateValue::new_with_metadata(bcs::to_bytes(&group).unwrap().into(), metadata.clone()),
        )]);

        let s = MockStateView::new(data);
        let resolver = as_resolver_with_group_size_kind(&s, GroupSizeKind::AsSum);

        let group_changes = BTreeMap::from([(
            mock_tag_2(),
            MoveStorageOp::New((vec![3, 3, 3].into(), None)),
        )]);
        let converter = WriteOpConverter::new(&resolver, true);
        let group_write = converter
            .convert_resource_group_v1(&key, group_changes)
            .unwrap();

        assert_eq!(group_write.metadata_op().metadata(), &metadata);
        let expected_new_size = group_size_as_sum(
            vec![(&mock_tag_0(), 1), (&mock_tag_1(), 2), (&mock_tag_2(), 3)].into_iter(),
        )
        .unwrap();
        assert_some_eq!(group_write.maybe_group_op_size(), expected_new_size);
        assert_eq!(group_write.inner_ops().len(), 1);
        assert_some_eq!(
            group_write.inner_ops().get(&mock_tag_2()),
            &(WriteOp::legacy_creation(vec![3, 3, 3].into()), None)
        );
    }

    // TODO[agg_v2](test) make as_resolver_with_group_size_kind support AsSum
    // #[test]
    #[allow(unused)]
    fn size_computation_new_group() {
        let s = MockStateView::new(BTreeMap::new());
        let resolver = as_resolver_with_group_size_kind(&s, GroupSizeKind::AsSum);

        // TODO[agg_v2](test): Layout hardcoded to None. Test with layout = Some(..)
        let group_changes =
            BTreeMap::from([(mock_tag_1(), MoveStorageOp::New((vec![2, 2].into(), None)))]);
        let key = StateKey::raw(&[0]);
        let converter = WriteOpConverter::new(&resolver, true);
        let group_write = converter
            .convert_resource_group_v1(&key, group_changes)
            .unwrap();

        assert!(group_write.metadata_op().metadata().is_none());
        let expected_new_size = group_size_as_sum(vec![(&mock_tag_1(), 2)].into_iter()).unwrap();
        assert_some_eq!(group_write.maybe_group_op_size(), expected_new_size);
        assert_eq!(group_write.inner_ops().len(), 1);
        assert_some_eq!(
            group_write.inner_ops().get(&mock_tag_1()),
            &(WriteOp::legacy_creation(vec![2, 2].into()), None)
        );
    }

    // TODO[agg_v2](test) make as_resolver_with_group_size_kind support AsSum
    // #[test]
    #[allow(unused)]
    fn size_computation_delete_group() {
        let group: BTreeMap<StructTag, Bytes> = BTreeMap::from([
            (mock_tag_0(), vec![1].into()),
            (mock_tag_1(), vec![2, 2].into()),
        ]);
        let metadata = raw_metadata(100);
        let key = StateKey::raw(&[0]);

        let data = BTreeMap::from([(
            key.clone(),
            StateValue::new_with_metadata(bcs::to_bytes(&group).unwrap().into(), metadata.clone()),
        )]);

        let s = MockStateView::new(data);
        let resolver = as_resolver_with_group_size_kind(&s, GroupSizeKind::AsSum);
        let group_changes = BTreeMap::from([
            (mock_tag_0(), MoveStorageOp::Delete),
            (mock_tag_1(), MoveStorageOp::Delete),
        ]);
        let converter = WriteOpConverter::new(&resolver, true);
        let group_write = converter
            .convert_resource_group_v1(&key, group_changes)
            .unwrap();

        // Deletion should still contain the metadata - for storage refunds.
        assert_eq!(group_write.metadata_op().metadata(), &metadata);
        assert_eq!(group_write.metadata_op(), &WriteOp::Deletion { metadata });
        assert_none!(group_write.metadata_op().bytes());
    }
}
