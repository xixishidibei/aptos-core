// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::Context;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use url::Url;

const IMPORTED_TRANSACTIONS_FOLDER: &str = "imported_transactions";
const SCRIPTED_TRANSACTIONS_FOLDER: &str = "scripted_transactions";

#[derive(Parser)]
pub struct IndexerCliArgs {
    /// Path to the configuration file with `TransactionGeneratorConfig`.
    #[clap(long)]
    pub config: PathBuf,

    #[clap(long)]
    pub move_folder_path: Option<PathBuf>,

    /// Path to the output folder where the generated transactions will be saved.
    #[clap(long)]
    pub output_folder: PathBuf,
}

impl IndexerCliArgs {
    pub async fn run(&self) -> anyhow::Result<()> {
        // Read the configuration file.
        let config_raw = tokio::fs::read_to_string(&self.config)
            .await
            .with_context(|| format!("Failed to read configuration file: {:?}", self.config))?;

        // Parse the configuration.
        let config: TransactionGeneratorConfig = serde_yaml::from_str(&config_raw)
            .with_context(|| format!("Failed to parse configuration file: {:?}", self.config))?;

        // Run the transaction generator.
        config
            .run(&self.move_folder_path, &self.output_folder)
            .await
    }
}

/// Overall configuration for the transaction generator.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionGeneratorConfig {
    // Configuration for importing transactions from multiple networks.
    pub import_config: TransactionImporterConfig,
}

impl TransactionGeneratorConfig {
    pub async fn run(
        &self,
        move_folder_path: &Option<PathBuf>,
        output_path: &Path,
    ) -> anyhow::Result<()> {
        let import_config_path = output_path.join(IMPORTED_TRANSACTIONS_FOLDER);
        // Check if the output folder exists.
        if !import_config_path.exists() {
            tokio::fs::create_dir_all(&import_config_path).await?;
        }
        self.import_config
            .run(&import_config_path)
            .await
            .context("Importing transactions failed.")?;

        if move_folder_path.is_none() {
            return Ok(());
        }

        let script_config_path = output_path.join(SCRIPTED_TRANSACTIONS_FOLDER);
        // Check if the output folder exists.
        if !script_config_path.exists() {
            tokio::fs::create_dir_all(&script_config_path).await?;
        }

        // Scan all yaml files in the move folder path.
        let mut script_transactions_vec: Vec<(String, ScriptTransactions)> = vec![];
        let move_files = std::fs::read_dir(move_folder_path.as_ref().unwrap())?;
        for entry in move_files {
            let entry = entry?;
            // entry has to be a file.
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().unwrap_or_default() == "yaml" {
                let file_name = path.file_name().unwrap().to_str().unwrap();
                let script_transactions_raw: String = tokio::fs::read_to_string(&path).await?;
                let script_transactions: ScriptTransactions =
                    serde_yaml::from_str(&script_transactions_raw)?;
                script_transactions_vec.push((file_name.to_string(), script_transactions));
            }
        }

        // Validate the configuration.
        let mut output_script_transactions_set = HashSet::new();
        for (file_name, script_transactions) in script_transactions_vec.iter() {
            if script_transactions.transactions.is_empty() {
                return Err(anyhow::anyhow!(
                    "[Script Transaction Generator] No transactions found in file `{}`",
                    file_name
                ));
            }
            for script_transaction in script_transactions.transactions.iter() {
                if let Some(output_name) = &script_transaction.output_name {
                    if !output_script_transactions_set.insert(output_name.clone()) {
                        return Err(anyhow::anyhow!(
                            "[Script Transaction Generator] Output file name `{}` is duplicated in file `{}`",
                            output_name.clone(),
                            file_name
                        ));
                    }
                }
            }
        }

        // Run each config.
        for (file_name, script_transactions) in script_transactions_vec {
            script_transactions
                .run(&script_config_path)
                .await
                .context(format!(
                    "Failed to generate script transaction for file `{}`",
                    file_name
                ))?;
        }

        Ok(())
    }
}

/// Configuration for importing transactions from multiple networks.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TransactionImporterConfig {
    // Config is a map from network name to the configuration for that network.
    #[serde(flatten)]
    pub configs: HashMap<String, TransactionImporterPerNetworkConfig>,
}

impl TransactionImporterConfig {
    fn validate(&self) -> anyhow::Result<()> {
        // Validate the configuration. This is to make sure that no output file shares the same name.
        let mut output_files = HashSet::new();
        for (_, network_config) in self.configs.iter() {
            for output_file in network_config.versions_to_import.values() {
                if !output_files.insert(output_file) {
                    return Err(anyhow::anyhow!(
                        "[Transaction Importer] Output file name {} is duplicated",
                        output_file
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn run(&self, output_path: &Path) -> anyhow::Result<()> {
        // Validate the configuration.
        self.validate()?;

        // Run the transaction importer for each network.
        for (network_name, network_config) in self.configs.iter() {
            network_config.run(output_path).await.context(format!(
                "[Transaction Importer] Failed for network: {}",
                network_name
            ))?;
        }
        Ok(())
    }
}

/// Configuration for importing transactions from a network.
/// This includes the URL of the network, the API key, the version of the transaction to fetch,
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionImporterPerNetworkConfig {
    /// The endpoint of the transaction stream.
    pub transaction_stream_endpoint: Url,
    /// The API key to use for the transaction stream if required.
    pub api_key: Option<String>,
    /// The version of the transaction to fetch and their output file names.
    pub versions_to_import: HashMap<u64, String>,
}

/// Configuration for generating transactions from a script.
/// `ScriptTransactions` will generate a list of transactions and output if specified.
/// A managed-node will be used to execute the scripts in sequence.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptTransactions {
    pub transactions: Vec<ScriptTransaction>,
}

/// A step that can optionally output one transaction.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptTransaction {
    pub script_path: PathBuf,
    pub output_name: Option<String>,
    // Optional address to fund the account; if not provided, the default profile address will be used.
    pub fund_address: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_import_transactions_duplicate_output_name() {
        let transaction_generator_config = r#"
            {
                "import_config": {
                    "mainnet": {
                        "transaction_stream_endpoint": "http://mainnet.com",
                        "api_key": "mainnet_api_key",
                        "versions_to_import": {
                            1: "mainnet_v1.json"
                        }
                    },
                    "testnet": {
                        "transaction_stream_endpoint": "http://testnet.com",
                        "api_key": "testnet_api_key",
                        "versions_to_import": {
                            1: "mainnet_v1.json"
                        }
                    }
                }
            }
        "#;
        let transaction_generator_config: TransactionGeneratorConfig =
            serde_yaml::from_str(transaction_generator_config).unwrap();
        let output_path = PathBuf::from("/tmp");
        let move_folder_path = None;
        let result = transaction_generator_config
            .run(&move_folder_path, &output_path)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_script_transactions_duplicate_output_name() {
        let transaction_generator_config = r#"
            {
                "import_config": {
                }
            }
        "#;
        let transaction_generator_config: TransactionGeneratorConfig =
            serde_yaml::from_str(transaction_generator_config).unwrap();
        let output_path = PathBuf::from("/tmp");
        let tempfile = tempfile::tempdir().unwrap();
        let move_folder_path = Some(tempfile.path().to_path_buf());
        let first_script_transactions = r#"
            transactions:
              - script_path: "simple_script_1"
                output_name: "output.json"
        "#;
        let second_script_transactions = r#"
            transactions:
              - script_path: "simple_script_2"
                output_name: "output.json"
        "#;
        let first_script_transactions_path = tempfile.path().join("first_script_transactions.yaml");
        let second_script_transactions_path =
            tempfile.path().join("second_script_transactions.yaml");
        tokio::fs::write(&first_script_transactions_path, first_script_transactions)
            .await
            .unwrap();
        tokio::fs::write(&second_script_transactions_path, second_script_transactions)
            .await
            .unwrap();
        let result = transaction_generator_config
            .run(&move_folder_path, &output_path)
            .await;
        assert!(result.is_err());
    }
}
