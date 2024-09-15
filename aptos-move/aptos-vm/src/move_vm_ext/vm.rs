// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    move_vm_ext::{warm_vm_cache::WarmVmCache, AptosMoveResolver, SessionExt, SessionId},
    natives::aptos_natives_with_builder,
};
use aptos_crypto::HashValue;
use aptos_gas_algebra::DynamicExpression;
use aptos_gas_schedule::{
    AptosGasParameters, MiscGasParameters, NativeGasParameters, LATEST_GAS_FEATURE_VERSION,
};
use aptos_native_interface::SafeNativeBuilder;
use aptos_types::{
    chain_id::ChainId,
    on_chain_config::{FeatureFlag, Features, TimedFeaturesBuilder},
    state_store::StateView,
    transaction::user_transaction_context::UserTransactionContext,
    vm::configs::aptos_prod_vm_config,
};
use aptos_vm_types::{
    environment::{
        aptos_default_ty_builder, aptos_prod_ty_builder, fetch_runtime_environment,
        set_runtime_environment, Environment,
    },
    module_and_script_storage::{AptosCodeStorageAdapter, AsAptosCodeStorage},
    storage::change_set_configs::ChangeSetConfigs,
};
use move_vm_runtime::move_vm::MoveVM;
use std::{ops::Deref, sync::Arc};

/// MoveVM wrapper which is used to run genesis initializations. Designed as a
/// stand-alone struct to ensure all genesis configurations are in one place,
/// and are modified accordingly. The VM is initialized with default parameters,
/// and should only be used to run genesis sessions.
pub struct GenesisMoveVM {
    vm: MoveVM,
    chain_id: ChainId,
    features: Features,
}

impl GenesisMoveVM {
    pub fn new(chain_id: ChainId) -> Self {
        let features = Features::default();
        let timed_features = TimedFeaturesBuilder::enable_all().build();

        let vm_config =
            aptos_prod_vm_config(&features, &timed_features, aptos_default_ty_builder());

        // All genesis sessions run with unmetered gas meter, and here we set
        // the gas parameters for natives as zeros (because they do not matter).
        let mut native_builder = SafeNativeBuilder::new(
            LATEST_GAS_FEATURE_VERSION,
            NativeGasParameters::zeros(),
            MiscGasParameters::zeros(),
            timed_features.clone(),
            features.clone(),
            None,
        );

        let vm = MoveVM::new_with_config(
            aptos_natives_with_builder(&mut native_builder, false),
            vm_config,
        );

        Self {
            vm,
            chain_id,
            features,
        }
    }

    pub fn as_aptos_code_storage<'s, S: StateView>(
        &'s self,
        state_view: &'s S,
    ) -> AptosCodeStorageAdapter<'s, S> {
        state_view.as_aptos_code_storage(&self.vm)
    }

    pub fn genesis_change_set_configs(&self) -> ChangeSetConfigs {
        // Because genesis sessions are not metered, there are no change set
        // (storage) costs as well.
        ChangeSetConfigs::unlimited_at_gas_feature_version(LATEST_GAS_FEATURE_VERSION)
    }

    pub fn new_genesis_session<'r, R: AptosMoveResolver>(
        &self,
        resolver: &'r R,
        genesis_id: HashValue,
    ) -> SessionExt<'r, '_> {
        let session_id = SessionId::genesis(genesis_id);
        SessionExt::new(
            session_id,
            &self.vm,
            self.chain_id,
            &self.features,
            None,
            resolver,
        )
    }
}

#[derive(Clone)]
pub struct MoveVmExt {
    inner: MoveVM,
    pub(crate) env: Arc<Environment>,
}

impl MoveVmExt {
    fn new_impl(
        gas_feature_version: u64,
        gas_params: Result<&AptosGasParameters, &String>,
        env: Arc<Environment>,
        gas_hook: Option<Arc<dyn Fn(DynamicExpression) + Send + Sync>>,
        inject_create_signer_for_gov_sim: bool,
        resolver: &impl AptosMoveResolver,
    ) -> Self {
        // TODO(Gas): Right now, we have to use some dummy values for gas parameters if they are not found on-chain.
        //            This only happens in a edge case that is probably related to write set transactions or genesis,
        //            which logically speaking, shouldn't be handled by the VM at all.
        //            We should clean up the logic here once we get that refactored.
        let (native_gas_params, misc_gas_params, ty_builder) = match gas_params {
            Ok(gas_params) => {
                let ty_builder = aptos_prod_ty_builder(gas_feature_version, gas_params);
                (
                    gas_params.natives.clone(),
                    gas_params.vm.misc.clone(),
                    ty_builder,
                )
            },
            Err(_) => {
                let ty_builder = aptos_default_ty_builder();
                (
                    NativeGasParameters::zeros(),
                    MiscGasParameters::zeros(),
                    ty_builder,
                )
            },
        };

        let mut builder = SafeNativeBuilder::new(
            gas_feature_version,
            native_gas_params,
            misc_gas_params,
            env.timed_features().clone(),
            env.features().clone(),
            gas_hook,
        );

        // TODO(George): Move gas configs to environment to avoid this clone!
        let mut vm_config = env.vm_config().clone();
        vm_config.ty_builder = ty_builder;
        vm_config.disallow_dispatch_for_native = env
            .features()
            .is_enabled(FeatureFlag::DISALLOW_USER_NATIVES);

        // TODO(loader_v2): Refresh environment when configs change.
        set_runtime_environment(
            vm_config.clone(),
            aptos_natives_with_builder(&mut builder, inject_create_signer_for_gov_sim),
        );
        let vm = if env.features().is_loader_v2_enabled() {
            // TODO(loader_v2): For now re-create the VM every time. Later we can have a
            //                  single VM created once, which also holds the environment.
            MoveVM::new_with_runtime_environment(fetch_runtime_environment())
        } else {
            WarmVmCache::get_warm_vm(
                builder,
                vm_config,
                resolver,
                env.features().is_enabled(FeatureFlag::VM_BINARY_FORMAT_V7),
                inject_create_signer_for_gov_sim,
            )
            .expect("should be able to create Move VM; check if there are duplicated natives")
        };

        Self { inner: vm, env }
    }

    pub fn new(
        gas_feature_version: u64,
        gas_params: Result<&AptosGasParameters, &String>,
        env: Arc<Environment>,
        resolver: &impl AptosMoveResolver,
    ) -> Self {
        Self::new_impl(gas_feature_version, gas_params, env, None, false, resolver)
    }

    pub fn new_with_extended_options(
        gas_feature_version: u64,
        gas_params: Result<&AptosGasParameters, &String>,
        env: Arc<Environment>,
        gas_hook: Option<Arc<dyn Fn(DynamicExpression) + Send + Sync>>,
        inject_create_signer_for_gov_sim: bool,
        resolver: &impl AptosMoveResolver,
    ) -> Self {
        Self::new_impl(
            gas_feature_version,
            gas_params,
            env,
            gas_hook,
            inject_create_signer_for_gov_sim,
            resolver,
        )
    }

    pub fn new_session<'r, R: AptosMoveResolver>(
        &self,
        resolver: &'r R,
        session_id: SessionId,
        maybe_user_transaction_context: Option<UserTransactionContext>,
    ) -> SessionExt<'r, '_> {
        SessionExt::new(
            session_id,
            &self.inner,
            self.env.chain_id(),
            self.env.features(),
            maybe_user_transaction_context,
            resolver,
        )
    }
}

impl Deref for MoveVmExt {
    type Target = MoveVM;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
