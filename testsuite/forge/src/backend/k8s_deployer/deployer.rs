// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{
    FORGE_DEPLOYER_IMAGE_TAG, FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME,
    FORGE_DEPLOYER_VALUES_ENV_VAR_NAME, FORGE_INDEXER_DEPLOYER_DOCKER_IMAGE_REPO,
    FORGE_TESTNET_DEPLOYER_DOCKER_IMAGE_REPO,
};
use crate::{maybe_create_k8s_resource, K8sApi, ReadWrite, Result};
use k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{ConfigMap, Namespace, ServiceAccount},
    rbac::v1::RoleBinding,
};
use kube::{
    api::{ObjectMeta, PostParams},
    ResourceExt,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Clone, Copy)]
pub enum ForgeDeployerType {
    Indexer,
    Testnet,
}

impl fmt::Display for ForgeDeployerType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ForgeDeployerType::Indexer => write!(f, "indexer"),
            ForgeDeployerType::Testnet => write!(f, "testnet"),
        }
    }
}

/// The ForgeDeployerManager is responsible for managing the lifecycle of forge deployers, which deploy the
/// forge components to the k8s cluster.
pub struct ForgeDeployerManager {
    // all the k8s APIs we need. Specifying each API separately allows for easier testing
    pub jobs_api: Arc<dyn ReadWrite<Job>>,
    pub config_maps_api: Arc<dyn ReadWrite<ConfigMap>>,
    pub namespace_api: Arc<dyn ReadWrite<Namespace>>,
    pub serviceaccount_api: Arc<dyn ReadWrite<ServiceAccount>>,
    pub rolebinding_api: Arc<dyn ReadWrite<RoleBinding>>,

    pub namespace: String,

    // the values to use for the deployer, including namespace, era, etc
    pub config: serde_json::Value,
}

impl ForgeDeployerManager {
    pub fn new(kube_client: kube::Client, namespace: String, config: serde_json::Value) -> Self {
        let jobs_api = Arc::new(K8sApi::from_client(
            kube_client.clone(),
            Some(namespace.clone()),
        ));
        let config_maps_api = Arc::new(K8sApi::from_client(
            kube_client.clone(),
            Some(namespace.clone()),
        ));
        let namespace_api = Arc::new(K8sApi::from_client(kube_client.clone(), None));
        let serviceaccount_api = Arc::new(K8sApi::from_client(
            kube_client.clone(),
            Some(namespace.clone()),
        ));
        let rolebinding_api = Arc::new(K8sApi::from_client(
            kube_client.clone(),
            Some(namespace.clone()),
        ));

        // ensure it lives long enough between async
        Self {
            jobs_api,
            config_maps_api,
            namespace_api,
            serviceaccount_api,
            rolebinding_api,
            namespace,
            config,
        }
    }

    /// Given a deployer type return the name to use for k8s components
    /// This is the canonical name for the deployer and each of its components
    pub(crate) fn get_name(&self, deployer_type: ForgeDeployerType) -> String {
        format!("deploy-forge-{}", deployer_type)
    }

    /// Builds a k8s configmap for the forge deployer that contains the values needed to deploy the forge components
    /// Does not actually create the configmap in k8s
    fn build_forge_deployer_k8s_config_map(
        &self,
        deployer_type: ForgeDeployerType,
    ) -> Result<ConfigMap> {
        let configmap_name = self.get_name(deployer_type);
        let deploy_values_json = serde_json::to_string(&self.config)?;

        // create the configmap with values
        let config_map = ConfigMap {
            metadata: ObjectMeta {
                name: Some(configmap_name.clone()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            data: Some(BTreeMap::from([(
                "deploy-values.json".to_string(),
                deploy_values_json,
            )])),
            ..Default::default()
        };

        Ok(config_map)
    }

    /// Builds a k8s job for the forge deployer that implements the particular interface that it expects:
    /// - Runs the corresponding forge-<component>-deployer image
    /// - Sets the FORGE_DEPLOY_VALUES_JSON environment variable to the configmap that contains the values
    /// Does not actually create the job in k8s
    fn build_forge_deployer_k8s_job(
        &self,
        deployer_type: ForgeDeployerType,
        configmap_name: String,
    ) -> Result<Job> {
        let job_name = self.get_name(deployer_type);
        let image_repo: &str = match deployer_type {
            ForgeDeployerType::Indexer => FORGE_INDEXER_DEPLOYER_DOCKER_IMAGE_REPO,
            ForgeDeployerType::Testnet => FORGE_TESTNET_DEPLOYER_DOCKER_IMAGE_REPO,
        };
        let image_tag: &str = FORGE_DEPLOYER_IMAGE_TAG;

        let job = Job {
            metadata: ObjectMeta {
                name: Some(job_name.clone()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::batch::v1::JobSpec {
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        service_account_name: Some(FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME.to_string()),
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "forge-deployer".to_string(),
                            image: Some(format!("{}:{}", image_repo, image_tag)),
                            env: Some(vec![k8s_openapi::api::core::v1::EnvVar {
                                name: FORGE_DEPLOYER_VALUES_ENV_VAR_NAME.to_string(),
                                value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                    config_map_key_ref: Some(
                                        k8s_openapi::api::core::v1::ConfigMapKeySelector {
                                            name: Some(configmap_name),
                                            key: "deploy-values.json".to_string(),
                                            ..Default::default()
                                        },
                                    ),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }],
                        restart_policy: Some("Never".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                backoff_limit: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(job)
    }

    pub async fn start(&self, deployer_type: ForgeDeployerType) -> Result<()> {
        self.ensure_namespace_prepared().await?;
        let config_map = self.build_forge_deployer_k8s_config_map(deployer_type)?;
        let job = self.build_forge_deployer_k8s_job(deployer_type, config_map.name())?;
        self.config_maps_api
            .create(&PostParams::default(), &config_map)
            .await?;
        self.jobs_api.create(&PostParams::default(), &job).await?;
        Ok(())
    }

    fn build_namespace(&self) -> Namespace {
        Namespace {
            metadata: ObjectMeta {
                name: Some(self.namespace.clone()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn build_service_account(&self) -> ServiceAccount {
        ServiceAccount {
            metadata: ObjectMeta {
                name: Some(FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME.to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn build_role_binding(&self) -> RoleBinding {
        RoleBinding {
            metadata: ObjectMeta {
                name: Some("forge-admin".to_string()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            role_ref: k8s_openapi::api::rbac::v1::RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "ClusterRole".to_string(),
                name: "cluster-admin".to_string(),
            },
            subjects: Some(vec![k8s_openapi::api::rbac::v1::Subject {
                kind: "ServiceAccount".to_string(),
                name: FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME.to_string(),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            }]),
        }
    }

    async fn ensure_namespace_prepared(&self) -> Result<()> {
        let namespace = self.build_namespace();
        maybe_create_k8s_resource(self.namespace_api.clone(), namespace.clone()).await?;
        let service_account = self.build_service_account();
        maybe_create_k8s_resource(self.serviceaccount_api.clone(), service_account).await?;
        let role_binding = self.build_role_binding();
        maybe_create_k8s_resource(self.rolebinding_api.clone(), role_binding).await?;
        Ok(())
    }

    pub async fn completed(&self, deployer_type: ForgeDeployerType) -> Result<bool> {
        let job_name = self.get_name(deployer_type);
        let job = self.jobs_api.get(&job_name).await?;
        Ok(job
            .status
            .expect("Failed to get job status")
            .succeeded
            .expect("Failed to get job succeeded number")
            > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockK8sResourceApi;
    use serde_json::json;

    /// Test creating a forge deployer manager and creating an indexer deployment with it. Nothing
    /// exists in the namespace yet
    #[tokio::test]
    async fn test_start_deployer_fresh_environment() {
        let namespace = "forge-large-banana".to_string();
        let config = serde_json::from_value(json!(
            {
                "profile": "large-banana",
                "era": "1",
                "namespace": namespace,
            }
        ))
        .expect("Issue creating Forge deployer config");
        let manager = ForgeDeployerManager {
            jobs_api: Arc::new(MockK8sResourceApi::new()),
            config_maps_api: Arc::new(MockK8sResourceApi::new()),
            namespace_api: Arc::new(MockK8sResourceApi::new()),
            serviceaccount_api: Arc::new(MockK8sResourceApi::new()),
            rolebinding_api: Arc::new(MockK8sResourceApi::new()),
            namespace,
            config,
        };
        manager.start(ForgeDeployerType::Indexer).await.unwrap();
        let indexer_deployer_name = manager.get_name(ForgeDeployerType::Indexer);
        manager
            .jobs_api
            .get(&indexer_deployer_name)
            .await
            .expect(format!("Expected job {} to exist", indexer_deployer_name).as_str());
        manager
            .config_maps_api
            .get(&indexer_deployer_name)
            .await
            .expect(format!("Expected configmap {} to exist", indexer_deployer_name).as_str());
    }

    /// Test starting a deployer with an existing job in the namespace. This should fail as the job already exists
    /// and we cannot override/mutate it.
    #[tokio::test]
    async fn test_start_deployer_existing_job() {
        let namespace = "forge-large-banana".to_string();
        let config = serde_json::from_value(json!(
            {
                "profile": "large-banana",
                "era": "1",
                "namespace": namespace,
            }
        ))
        .expect("Issue creating Forge deployer config");
        let manager = ForgeDeployerManager {
            jobs_api: Arc::new(MockK8sResourceApi::from_resource(Job {
                metadata: ObjectMeta {
                    name: Some("deploy-forge-indexer".to_string()),
                    namespace: Some("default".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })),
            config_maps_api: Arc::new(MockK8sResourceApi::new()),
            namespace_api: Arc::new(MockK8sResourceApi::new()),
            serviceaccount_api: Arc::new(MockK8sResourceApi::new()),
            rolebinding_api: Arc::new(MockK8sResourceApi::new()),
            namespace,
            config,
        };
        let result = manager.start(ForgeDeployerType::Indexer).await;
        assert!(result.is_err());
    }

    /// Test starting a deployer with an existing job in the namespace but a different era. This should be allowed
    /// as the new job/deployment will be in a different era and unrelated to the existing job
    #[tokio::test]
    async fn test_start_deployer_existing_job_different_era() {
        let namespace = "forge-large-banana".to_string();
        let config = serde_json::from_value(json!(
            {
                "profile": "large-banana",
                "era": "1",
                "namespace": namespace,
            }
        ))
        .expect("Issue creating Forge deployer config");
        let manager = ForgeDeployerManager {
            jobs_api: Arc::new(MockK8sResourceApi::from_resource(Job {
                metadata: ObjectMeta {
                    name: Some("deploy-forge-indexer-e1".to_string()),
                    namespace: Some("default".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })),
            config_maps_api: Arc::new(MockK8sResourceApi::new()),
            namespace_api: Arc::new(MockK8sResourceApi::new()),
            serviceaccount_api: Arc::new(MockK8sResourceApi::new()),
            rolebinding_api: Arc::new(MockK8sResourceApi::new()),
            namespace,
            config,
        };
        manager.start(ForgeDeployerType::Indexer).await.unwrap();
    }

    /// Test ensure_namespace_prepared creates the namespace, serviceaccount, and rolebinding
    /// Collisions should be OK to ensure idempotency
    #[tokio::test]
    async fn test_ensure_namespace_prepared_fresh_namespace() {
        let namespace = "forge-large-banana".to_string();
        let config = serde_json::from_value(json!(
            {
                "profile": "large-banana",
                "era": "1",
                "namespace": namespace,
            }
        ))
        .expect("Issue creating Forge deployer config");
        let manager = ForgeDeployerManager {
            jobs_api: Arc::new(MockK8sResourceApi::new()),
            config_maps_api: Arc::new(MockK8sResourceApi::new()),
            namespace_api: Arc::new(MockK8sResourceApi::new()),
            serviceaccount_api: Arc::new(MockK8sResourceApi::new()),
            rolebinding_api: Arc::new(MockK8sResourceApi::new()),
            namespace,
            config,
        };
        manager
            .ensure_namespace_prepared()
            .await
            .expect("Issue ensuring namespace prepared");
        let namespace = manager
            .namespace_api
            .get("forge-large-banana")
            .await
            .expect(format!("Expected namespace {} to exist", "forge-large-banana").as_str());
        assert_eq!(
            namespace.metadata.name,
            Some("forge-large-banana".to_string())
        );
        let serviceaccount = manager
            .serviceaccount_api
            .get(FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME)
            .await
            .expect(
                format!(
                    "Expected serviceaccount {} to exist",
                    FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME
                )
                .as_str(),
            );
        assert_eq!(
            serviceaccount.metadata.name,
            Some(FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME.to_string())
        );
        let rolebinding = manager.rolebinding_api.get("forge-admin").await.unwrap();
        assert_eq!(rolebinding.metadata.name, Some("forge-admin".to_string()));
    }

    /// Test the same thing but with existing resources. This should not error out and should be idempotent
    #[tokio::test]
    async fn test_ensure_namespace_prepared_existing_resources() {
        let namespace = "forge-large-banana".to_string();
        let config = serde_json::from_value(json!(
            {
                "profile": "large-banana",
                "era": "1",
                "namespace": namespace,
            }
        ))
        .expect("Issue creating Forge deployer config");
        let manager = ForgeDeployerManager {
            jobs_api: Arc::new(MockK8sResourceApi::new()),
            config_maps_api: Arc::new(MockK8sResourceApi::new()),
            namespace_api: Arc::new(MockK8sResourceApi::from_resource(Namespace {
                metadata: ObjectMeta {
                    name: Some("forge-large-banana".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })),
            serviceaccount_api: Arc::new(MockK8sResourceApi::from_resource(ServiceAccount {
                metadata: ObjectMeta {
                    name: Some(FORGE_DEPLOYER_SERVICE_ACCOUNT_NAME.to_string()),
                    namespace: Some("forge-large-banana".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })),
            rolebinding_api: Arc::new(MockK8sResourceApi::from_resource(RoleBinding {
                metadata: ObjectMeta {
                    name: Some("forge-admin".to_string()),
                    namespace: Some("forge-large-banana".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            })),
            namespace,
            config,
        };
        manager
            .ensure_namespace_prepared()
            .await
            .expect("Issue ensuring namespace prepared");
    }
}
