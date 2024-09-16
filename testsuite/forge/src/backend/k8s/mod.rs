// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{Factory, GenesisConfig, GenesisConfigFn, NodeConfigFn, Result, Swarm, Version};
use anyhow::bail;
use aptos_logger::info;
use rand::rngs::StdRng;
use std::{convert::TryInto, num::NonZeroUsize, time::Duration};

pub mod chaos;
pub mod chaos_schema;
mod cluster_helper;
pub mod constants;
mod fullnode;
pub mod kube_api;
pub mod node;
pub mod prometheus;
mod stateful_set;
mod swarm;

use super::{
    ForgeDeployerManager, ForgeDeployerType, ForgeDeployerValues, DEFAULT_FORGE_DEPLOYER_PROFILE,
};
use aptos_sdk::crypto::ed25519::ED25519_PRIVATE_KEY_LENGTH;
pub use cluster_helper::*;
pub use constants::*;
pub use fullnode::*;
#[cfg(test)]
pub use kube_api::mocks::*;
pub use kube_api::*;
pub use node::K8sNode;
pub use stateful_set::*;
pub use swarm::*;

pub struct K8sFactory {
    root_key: [u8; ED25519_PRIVATE_KEY_LENGTH],
    image_tag: String,
    upgrade_image_tag: String,
    kube_namespace: String,
    use_port_forward: bool,
    reuse: bool,
    keep: bool,
    enable_haproxy: bool,
    enable_indexer: bool,
}

impl K8sFactory {
    pub fn new(
        kube_namespace: String,
        image_tag: String,
        upgrade_image_tag: String,
        use_port_forward: bool,
        reuse: bool,
        keep: bool,
        enable_haproxy: bool,
        enable_indexer: bool,
    ) -> Result<K8sFactory> {
        let root_key: [u8; ED25519_PRIVATE_KEY_LENGTH] =
            hex::decode(DEFAULT_ROOT_PRIV_KEY)?.try_into().unwrap();

        match kube_namespace.as_str() {
            "default" => {
                info!("Using the default kubernetes namespace");
            },
            s if s.starts_with("forge") => {
                info!("Using forge namespace: {}", s);
            },
            _ => {
                bail!(
                    "Invalid kubernetes namespace provided: {}. Use forge-*",
                    kube_namespace
                );
            },
        }

        Ok(Self {
            root_key,
            image_tag,
            upgrade_image_tag,
            kube_namespace,
            use_port_forward,
            reuse,
            keep,
            enable_haproxy,
            enable_indexer,
        })
    }
}

#[async_trait::async_trait]
impl Factory for K8sFactory {
    fn versions<'a>(&'a self) -> Box<dyn Iterator<Item = Version> + 'a> {
        let version = vec![
            Version::new(0, self.image_tag.clone()),
            Version::new(1, self.upgrade_image_tag.clone()),
        ];
        Box::new(version.into_iter())
    }

    async fn launch_swarm(
        &self,
        _rng: &mut StdRng,
        num_validators: NonZeroUsize,
        num_fullnodes: usize,
        init_version: &Version,
        genesis_version: &Version,
        genesis_config: Option<&GenesisConfig>,
        cleanup_duration: Duration,
        genesis_config_fn: Option<GenesisConfigFn>,
        node_config_fn: Option<NodeConfigFn>,
        existing_db_tag: Option<String>,
    ) -> Result<Box<dyn Swarm>> {
        let genesis_modules_path = match genesis_config {
            Some(config) => match config {
                GenesisConfig::Bundle(_) => {
                    bail!("k8s forge backend does not support raw bytes as genesis modules. please specify a path instead")
                },
                GenesisConfig::Path(path) => Some(path.clone()),
            },
            None => None,
        };

        let kube_client = create_k8s_client().await?;
        let (new_era, validators, fullnodes) = if self.reuse {
            let (validators, fullnodes) = match collect_running_nodes(
                &kube_client,
                self.kube_namespace.clone(),
                self.use_port_forward,
                self.enable_haproxy,
            )
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    bail!(e);
                },
            };
            let new_era = None; // TODO: get the actual era
            (new_era, validators, fullnodes)
        } else {
            // clear the cluster of resources
            delete_k8s_resources(kube_client.clone(), &self.kube_namespace).await?;
            // create the forge-management configmap before installing anything
            create_management_configmap(self.kube_namespace.clone(), self.keep, cleanup_duration)
                .await?;
            if let Some(existing_db_tag) = existing_db_tag {
                // TODO(prod-eng): For now we are managing PVs out of forge, and bind them manually
                // with the volume. Going forward we should consider automate this process.

                // The previously claimed PVs are in Released stage once the corresponding PVC is
                // gone. We reset its status to Available so they can be reused later.
                reset_persistent_volumes(&kube_client).await?;

                // We return early here if there are not enough PVs to claim.
                check_persistent_volumes(
                    kube_client.clone(),
                    num_validators.get() + num_fullnodes,
                    existing_db_tag,
                )
                .await?;
            }
            // try installing testnet resources, but clean up if it fails
            let (new_era, validators, fullnodes) = match install_testnet_resources(
                self.kube_namespace.clone(),
                num_validators.get(),
                num_fullnodes,
                format!("{}", init_version),
                format!("{}", genesis_version),
                genesis_modules_path,
                self.use_port_forward,
                self.enable_haproxy,
                genesis_config_fn,
                node_config_fn,
            )
            .await
            {
                Ok(res) => (Some(res.0), res.1, res.2),
                Err(e) => {
                    uninstall_testnet_resources(self.kube_namespace.clone()).await?;
                    bail!(e);
                },
            };

            // add an indexer too!
            if self.enable_indexer {
                // NOTE: by default, use a deploy profile and no additional configuration values
                let values = ForgeDeployerValues {
                    profile: DEFAULT_FORGE_DEPLOYER_PROFILE.to_string(),
                    era: new_era.clone().expect("Era not set in created testnet"),
                    namespace: self.kube_namespace.clone(),
                    indexer_grpc_values: None,
                    indexer_processor_values: None,
                };

                let forge_deployer_manager =
                    ForgeDeployerManager::from_k8s_client(kube_client.clone(), values);

                forge_deployer_manager.ensure_namespace_prepared().await?;
                forge_deployer_manager
                    .start(ForgeDeployerType::Indexer)
                    .await?;
            }

            (new_era, validators, fullnodes)
        };

        let swarm = K8sSwarm::new(
            &self.root_key,
            &self.image_tag,
            &self.upgrade_image_tag,
            &self.kube_namespace,
            validators,
            fullnodes,
            self.keep,
            new_era,
            self.use_port_forward,
        )
        .await
        .unwrap();
        Ok(Box::new(swarm))
    }
}
