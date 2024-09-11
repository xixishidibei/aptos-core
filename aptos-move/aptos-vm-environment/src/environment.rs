// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{gas::get_gas_parameters, natives::aptos_natives_with_builder};
use aptos_gas_algebra::DynamicExpression;
use aptos_gas_schedule::{
    gas_feature_versions::RELEASE_V1_15, AptosGasParameters, MiscGasParameters, NativeGasParameters,
};
use aptos_native_interface::SafeNativeBuilder;
use aptos_types::{
    chain_id::ChainId,
    on_chain_config::{
        ConfigurationResource, FeatureFlag, Features, OnChainConfig, TimedFeatures,
        TimedFeaturesBuilder,
    },
    state_store::StateView,
    vm::configs::{aptos_prod_vm_config, get_timed_feature_override},
};
use aptos_vm_types::storage::StorageGasParameters;
use move_vm_runtime::{
    config::VMConfig, use_loader_v1_based_on_env, RuntimeEnvironment, WithRuntimeEnvironment,
};
use move_vm_types::loaded_data::runtime_types::TypeBuilder;
use std::sync::Arc;

// TODO(George): move configs here from types crate.
pub fn aptos_prod_ty_builder(
    gas_feature_version: u64,
    gas_params: &AptosGasParameters,
) -> TypeBuilder {
    if gas_feature_version >= RELEASE_V1_15 {
        let max_ty_size = gas_params.vm.txn.max_ty_size;
        let max_ty_depth = gas_params.vm.txn.max_ty_depth;
        TypeBuilder::with_limits(max_ty_size.into(), max_ty_depth.into())
    } else {
        aptos_default_ty_builder()
    }
}

pub fn aptos_default_ty_builder() -> TypeBuilder {
    // Type builder to use when:
    //   1. Type size gas parameters are not yet in gas schedule (before 1.15).
    //   2. No gas parameters are found on-chain.
    TypeBuilder::with_limits(128, 20)
}

/// A runtime environment which can be used for VM initialization and more.
#[derive(Clone)]
pub struct Environment {
    chain_id: ChainId,

    features: Features,
    timed_features: TimedFeatures,

    gas_feature_version: u64,
    gas_params: Result<AptosGasParameters, String>,
    storage_gas_params: Result<StorageGasParameters, String>,

    runtime_environment: RuntimeEnvironment,

    inject_create_signer_for_gov_sim: bool,
}

impl Environment {
    pub fn new(
        state_view: &impl StateView,
        inject_create_signer_for_gov_sim: bool,
        gas_hook: Option<Arc<dyn Fn(DynamicExpression) + Send + Sync>>,
    ) -> Self {
        let mut features = Features::fetch_config(state_view).unwrap_or_default();

        // TODO(loader_v2): Remove before rolling out. This allows us to replay with V2.
        if use_loader_v1_based_on_env() {
            features.disable(FeatureFlag::ENABLE_LOADER_V2);
        } else {
            features.enable(FeatureFlag::ENABLE_LOADER_V2);
        }

        // If no chain ID is in storage, we assume we are in a testing environment.
        let chain_id = ChainId::fetch_config(state_view).unwrap_or_else(ChainId::test);
        let timestamp = ConfigurationResource::fetch_config(state_view)
            .map(|config| config.last_reconfiguration_time())
            .unwrap_or(0);

        let mut timed_features_builder = TimedFeaturesBuilder::new(chain_id, timestamp);
        if let Some(profile) = get_timed_feature_override() {
            timed_features_builder = timed_features_builder.with_override_profile(profile)
        }
        let timed_features = timed_features_builder.build();

        // TODO(Gas): Right now, we have to use some dummy values for gas parameters if they are not found on-chain.
        //            This only happens in a edge case that is probably related to write set transactions or genesis,
        //            which logically speaking, shouldn't be handled by the VM at all.
        //            We should clean up the logic here once we get that refactored.
        let (gas_params, storage_gas_params, gas_feature_version) =
            get_gas_parameters(&features, state_view);
        let (native_gas_params, misc_gas_params, ty_builder) = match &gas_params {
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
            timed_features.clone(),
            features.clone(),
            gas_hook,
        );
        let natives = aptos_natives_with_builder(&mut builder, inject_create_signer_for_gov_sim);
        let vm_config = aptos_prod_vm_config(&features, &timed_features, ty_builder);
        let runtime_environment = RuntimeEnvironment::new_with_config(natives, vm_config);

        Self {
            chain_id,
            features,
            timed_features,
            gas_feature_version,
            gas_params,
            storage_gas_params,
            runtime_environment,
            inject_create_signer_for_gov_sim,
        }
    }

    pub fn try_enable_delayed_field_optimization(mut self) -> Self {
        if self.features.is_aggregator_v2_delayed_fields_enabled() {
            self.runtime_environment.enable_delayed_field_optimization();
        }
        self
    }

    #[inline]
    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    #[inline]
    pub fn features(&self) -> &Features {
        &self.features
    }

    #[inline]
    pub fn timed_features(&self) -> &TimedFeatures {
        &self.timed_features
    }

    #[inline]
    pub fn vm_config(&self) -> &VMConfig {
        self.runtime_environment.vm_config()
    }

    #[inline]
    pub fn gas_feature_version(&self) -> u64 {
        self.gas_feature_version
    }

    #[inline(always)]
    pub fn gas_params(&self) -> &Result<AptosGasParameters, String> {
        &self.gas_params
    }

    #[inline(always)]
    pub fn storage_gas_params(&self) -> &Result<StorageGasParameters, String> {
        &self.storage_gas_params
    }

    #[inline(always)]
    pub fn inject_create_signer_for_gov_sim(&self) -> bool {
        self.inject_create_signer_for_gov_sim
    }
}

impl WithRuntimeEnvironment for Environment {
    fn runtime_environment(&self) -> &RuntimeEnvironment {
        &self.runtime_environment
    }
}

pub struct SharedEnvironment(pub Arc<Environment>);

impl Clone for SharedEnvironment {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl WithRuntimeEnvironment for SharedEnvironment {
    fn runtime_environment(&self) -> &RuntimeEnvironment {
        &self.0.runtime_environment
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use aptos_language_e2e_tests::data_store::FakeDataStore;

    #[test]
    fn test_new_environment() {
        // This creates an empty state.
        let state_view = FakeDataStore::default();
        let env = Environment::new(&state_view, false, None);

        // Check default values.
        assert_eq!(&env.features, &Features::default());
        assert_eq!(env.chain_id.id(), ChainId::test().id());
        assert!(!env.vm_config().delayed_field_optimization_enabled);

        let env = env.try_enable_delayed_field_optimization();
        assert!(env.vm_config().delayed_field_optimization_enabled);
    }

    // TODO(loader_v2): Re-activate?
    // #[test]
    // fn test_environment_for_testing() {
    //     let env = Environment::testing(ChainId::new(55));
    //
    //     assert_eq!(&env.features, &Features::default());
    //     assert_eq!(env.chain_id.id(), 55);
    //     assert!(!env.vm_config().delayed_field_optimization_enabled);
    //
    //     let expected_timed_features = TimedFeaturesBuilder::enable_all()
    //         .with_override_profile(TimedFeatureOverride::Testing)
    //         .build();
    //     assert_eq!(&env.timed_features, &expected_timed_features);
    // }
}
