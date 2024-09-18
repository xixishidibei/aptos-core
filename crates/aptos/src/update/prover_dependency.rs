// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cli_build_information,
    common::types::{CliCommand, CliError, CliTypedResult},
    update::{get_additional_binaries_dir, update_helper::get_arch},
};
use anyhow::{Context, Result};
use aptos_build_info::BUILD_OS;
use async_trait::async_trait;
use clap::Parser;
use dirs::home_dir;
use futures::StreamExt;
use move_prover_boogie_backend::options::{
    BoogieOptions, MAX_BOOGIE_VERSION, MAX_CVC5_VERSION, MAX_Z3_VERSION, MIN_BOOGIE_VERSION,
    MIN_CVC5_VERSION, MIN_Z3_VERSION,
};
use reqwest::Client;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    env,
    fs::{OpenOptions, Permissions},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use tokio::fs::{self};

const GITHUB_URL: &str = "https://github.com/";
const REPO_NAME: &str = "prover-dependency";
const REPO_OWNER: &str = "aptos-labs";

const BOOGIE_BINARY_NAME: &str = "boogie";
const TARGET_BOOGIE_VERSION: &str = "3.2.4";

const BOOGIE_EXE_ENV: &str = "BOOGIE_EXE";
#[cfg(target_os = "windows")]
const BOOGIE_EXE: &str = "boogie.exe";
#[cfg(not(target_os = "windows"))]
const BOOGIE_EXE: &str = "boogie";

const Z3_BINARY_NAME: &str = "z3";
const TARGET_Z3_VERSION: &str = "4.11.2";

const Z3_EXE_ENV: &str = "Z3_EXE";
#[cfg(target_os = "windows")]
const Z3_EXE: &str = "z3.exe";
#[cfg(not(target_os = "windows"))]
const Z3_EXE: &str = "z3";

const CVC5_BINARY_NAME: &str = "cvc5";
const TARGET_CVC5_VERSION: &str = "0.0.3";

const CVC5_EXE_ENV: &str = "CVC5_EXE";
#[cfg(target_os = "windows")]
const CVC5_EXE: &str = "cvc5.exe";
#[cfg(not(target_os = "windows"))]
const CVC5_EXE: &str = "cvc5";

/// Install dependencies (boogie, z3 and cvc5) for Move prover
#[derive(Debug, Parser)]
pub struct ProverDependencyInstaller {
    /// Where to install binaries of boogie, z3 and cvc5. If not
    /// given we will put it in a standard location for your OS.
    #[clap(long)]
    install_dir: Option<PathBuf>,
}

impl ProverDependencyInstaller {
    fn add_env_var(&self, env_var: &str, install_path: &Path) -> Result<(), CliError> {
        if let Ok(current_value) = env::var(env_var) {
            if current_value == install_path.to_string_lossy() {
                return Ok(());
            } else {
                println!(
                    "{} is already set to a different value: {}, please set it manually.",
                    env_var, current_value
                );
                return Ok(());
            }
        }

        if cfg!(target_os = "windows") {
            self.set_env_on_windows(env_var, install_path)?;
        } else {
            self.set_env_on_unix(env_var, install_path)?;
        }
        Ok(())
    }

    fn set_env_on_unix(&self, env_var: &str, install_path: &Path) -> Result<(), CliError> {
        let mut profile_path = home_dir().ok_or(CliError::UnexpectedError(
            "Failed to locate home directory".to_string(),
        ))?;
        profile_path.push(".profile");

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&profile_path)
            .map_err(|e| CliError::UnexpectedError(format!("Failed to open .profile: {}", e)))?;

        let line_to_add = format!(
            "export {}=\"{}\"\n",
            env_var,
            install_path.to_string_lossy()
        );

        file.write_all(line_to_add.as_bytes()).map_err(|e| {
            CliError::UnexpectedError(format!("Failed to write to .profile: {}", e))
        })?;

        println!(
            "Added {} to {:?} with value: {}.",
            env_var,
            profile_path,
            install_path.to_string_lossy()
        );

        Ok(())
    }

    fn set_env_on_windows(&self, env_var: &str, install_path: &Path) -> Result<(), CliError> {
        let output = Command::new("setx")
            .arg(env_var)
            .arg(install_path)
            .output()
            .map_err(|e| CliError::UnexpectedError(format!("Failed to run setx command: {}", e)))?;

        if !output.status.success() {
            return Err(CliError::UnexpectedError(format!(
                "Failed to set environment variable: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        println!(
            "Added {} to the environment with value: {}. ",
            env_var,
            install_path.to_string_lossy()
        );
        Ok(())
    }

    fn check_bin_version(
        &self,
        install_path: &Path,
        binary_name: &str,
        version: &str,
        option: &str,
    ) -> Result<(), String> {
        if !Path::new(&install_path).exists() {
            return Err(format!("{} not found.", install_path.to_string_lossy()));
        }

        let output = Command::new(install_path).arg(option).output();

        match output {
            Ok(output) => {
                let version_output = String::from_utf8_lossy(&output.stdout);
                // Check if the version output contains the expected version
                if version_output.contains(version) {
                    println!("{} {} already installed", binary_name, version);
                    Ok(())
                } else {
                    Err(format!(
                        "{} version mismatch. Expected: {}, Found: {}",
                        binary_name, version, version_output
                    ))
                }
            },
            Err(e) => Err(format!(
                "Failed to run {}: {}",
                install_path.to_string_lossy(),
                e
            )),
        }
    }

    async fn download_dependency(&self) -> CliTypedResult<String> {
        let arch = get_arch();
        let build_info = cli_build_information();
        let target_res = match build_info.get(BUILD_OS).context("Failed to determine build info of current CLI")?.as_str() {
            "linux-x86_64" => Ok("linux"),
            "macos-aarch64" | "macos-x86_64" => Ok("macos"),
            "windows-x86_64" => Ok("win"),
            wildcard => Err(CliError::UnexpectedError(format!("Self-updating is not supported on your OS ({}) right now, please download the binary manually", wildcard))),
        };

        let target = target_res?;

        let install_dir = match self.install_dir.clone() {
            Some(dir) => dir,
            None => {
                let dir = get_additional_binaries_dir();
                // Make the directory if it doesn't already exist.
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("Failed to create directory: {:?}", dir))?;
                dir
            },
        };

        let dir_name = format!("{}-{}", target, arch);

        BoogieOptions::check_version_is_compatible(
            BOOGIE_BINARY_NAME,
            TARGET_BOOGIE_VERSION,
            MIN_BOOGIE_VERSION,
            MAX_BOOGIE_VERSION,
        )?;
        self.install_binary(
            &install_dir,
            BOOGIE_EXE,
            BOOGIE_BINARY_NAME,
            TARGET_BOOGIE_VERSION,
            "/version",
            BOOGIE_EXE_ENV,
            &dir_name,
        )
        .await?;

        BoogieOptions::check_version_is_compatible(
            Z3_BINARY_NAME,
            TARGET_Z3_VERSION,
            MIN_Z3_VERSION,
            MAX_Z3_VERSION,
        )?;
        self.install_binary(
            &install_dir,
            Z3_EXE,
            Z3_BINARY_NAME,
            TARGET_Z3_VERSION,
            "--version",
            Z3_EXE_ENV,
            &dir_name,
        )
        .await?;

        BoogieOptions::check_version_is_compatible(
            CVC5_BINARY_NAME,
            TARGET_CVC5_VERSION,
            MIN_CVC5_VERSION,
            MAX_CVC5_VERSION,
        )?;
        self.install_binary(
            &install_dir,
            CVC5_EXE,
            CVC5_BINARY_NAME,
            TARGET_CVC5_VERSION,
            "--version",
            CVC5_EXE_ENV,
            &dir_name,
        )
        .await?;

        Ok("Installation succeeded".to_string())
    }

    async fn install_binary(
        &self,
        path: &Path,
        exe_name: &str,
        binary_name: &str,
        version: &str,
        version_option: &str,
        env_name: &str,
        dir_name: &str,
    ) -> CliTypedResult<String> {
        let install_dir = path.join(exe_name);
        if self
            .check_bin_version(&install_dir, binary_name, version, version_option)
            .is_err()
        {
            let download_url = format!(
                "{}/{}/{}/raw/main/{}/{}/{}/{}",
                GITHUB_URL, REPO_OWNER, REPO_NAME, binary_name, version, dir_name, exe_name
            );
            self.download_file(&download_url, &install_dir).await?;
        }
        if self.add_env_var(env_name, &install_dir).is_err() {
            println!(
                "Failed to set the environment variable {}. Please set it manually",
                env_name
            );
        }
        Ok(format!("{} is successfully installed", binary_name))
    }

    async fn download_file(&self, url: &str, path: &Path) -> CliTypedResult<String> {
        let client = Client::new();
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| CliError::ApiError(format!("Failed to send request: {}", e)))?;

        if !response.status().is_success() {
            return Err(CliError::ApiError(format!(
                "Failed to download file: HTTP {}",
                response.status()
            )));
        }

        let mut content = response.bytes_stream();

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(|e| {
                CliError::IO(
                    format!("Failed to open the file at:{}", path.to_string_lossy()),
                    e,
                )
            })?;

        while let Some(chunk) = content.next().await {
            let chunk =
                chunk.map_err(|e| CliError::ApiError(format!("Failed to read chunk: {}", e)))?;
            file.write_all(&chunk).map_err(|e| {
                CliError::IO(
                    format!("Failed to write to the file at:{}", path.to_string_lossy()),
                    e,
                )
            })?;
        }

        drop(file);

        #[cfg(unix)]
        {
            let permissions = Permissions::from_mode(0o755);
            fs::set_permissions(path, permissions).await.map_err(|e| {
                CliError::IO(
                    format!(
                        "Failed to set the permission to the file {}",
                        path.to_string_lossy()
                    ),
                    e,
                )
            })?;
        }

        Ok(path.to_string_lossy().to_string())
    }
}

#[async_trait]
impl CliCommand<String> for ProverDependencyInstaller {
    fn command_name(&self) -> &'static str {
        "InstallProverDependency"
    }

    async fn execute(self) -> CliTypedResult<String> {
        self.download_dependency().await
    }
}
