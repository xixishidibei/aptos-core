// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    CodeStorage, Module, ModuleStorage, RuntimeEnvironment, Script, WithRuntimeEnvironment,
};
use bytes::Bytes;
use move_binary_format::{
    errors::{Location, PartialVMError, VMResult},
    file_format::CompiledScript,
    CompiledModule,
};
use move_core_types::{
    account_address::AccountAddress, identifier::IdentStr, metadata::Metadata,
    vm_status::StatusCode,
};
use std::sync::Arc;

/// An error which is returned in case unreachable code is reached. This is just a safety
/// precaution to avoid panics in case we forget some gating.
macro_rules! unreachable_error {
    () => {
        Err(
            PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                .with_message(
                    "Loader V1 implementation should never use module or script storage"
                        .to_string(),
                )
                .finish(Location::Undefined),
        )
    };
}

/// Implementation of code storage (for modules and scripts) traits, to be used in case VM
/// is using V1 loader implementation. For example [Session::execute_entry_function] has
/// to take a reference to loader V2 storage interfaces, even if VM uses V1 loader. In this
/// case, they would be just unreachable.
pub struct UnreachableCodeStorage;

impl WithRuntimeEnvironment for UnreachableCodeStorage {
    fn runtime_environment(&self) -> &RuntimeEnvironment {
        // TODO(loader_v2): Double check this can never be called.
        unreachable!("Should not be called for unreachable storage")
    }
}

impl ModuleStorage for UnreachableCodeStorage {
    fn check_module_exists(
        &self,
        _address: &AccountAddress,
        _module_name: &IdentStr,
    ) -> VMResult<bool> {
        unreachable_error!()
    }

    fn fetch_module_bytes(
        &self,
        _address: &AccountAddress,
        _module_name: &IdentStr,
    ) -> VMResult<Option<Bytes>> {
        unreachable_error!()
    }

    fn fetch_module_size_in_bytes(
        &self,
        _address: &AccountAddress,
        _module_name: &IdentStr,
    ) -> VMResult<Option<usize>> {
        unreachable_error!()
    }

    fn fetch_module_metadata(
        &self,
        _address: &AccountAddress,
        _module_name: &IdentStr,
    ) -> VMResult<Option<Vec<Metadata>>> {
        unreachable_error!()
    }

    fn fetch_deserialized_module(
        &self,
        _address: &AccountAddress,
        _module_name: &IdentStr,
    ) -> VMResult<Option<Arc<CompiledModule>>> {
        unreachable_error!()
    }

    fn fetch_verified_module(
        &self,
        _address: &AccountAddress,
        _module_name: &IdentStr,
    ) -> VMResult<Option<Arc<Module>>> {
        unreachable_error!()
    }
}

impl CodeStorage for UnreachableCodeStorage {
    fn deserialize_and_cache_script(
        &self,
        _serialized_script: &[u8],
    ) -> VMResult<Arc<CompiledScript>> {
        unreachable_error!()
    }

    fn verify_and_cache_script(&self, _serialized_script: &[u8]) -> VMResult<Arc<Script>> {
        unreachable_error!()
    }
}
