use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use anyhow::Result;
use async_trait::async_trait;
#[cfg(feature = "mcp-http")]
use rmcp::transport::auth::{CredentialStore, InMemoryCredentialStore};
#[cfg(feature = "mcp-http")]
use rmcp::transport::AuthorizationManager;

use super::ExtensionManager;
use crate::action_required_manager::ActionRequiredManager;
use crate::container::Container;
use crate::extension::{ExtensionConfig, ExtensionResult, DEFAULT_EXTENSION_TIMEOUT};
use crate::mcp_client::{McpClientTrait, TraceContextInjector};

pub type SpawnServerFn = fn(tokio::io::DuplexStream, tokio::io::DuplexStream);

#[derive(Clone, Copy, Default)]
pub struct PlatformExtension {
    pub unprefixed_tools: bool,
}

#[derive(Clone)]
pub struct ExtensionSession {
    pub id: String,
    pub working_dir: PathBuf,
    pub container: Option<Container>,
    pub extensions: Vec<ExtensionConfig>,
    pub provider_runs_tool_loop: bool,
}

#[cfg(feature = "mcp-http")]
#[derive(Clone, Debug, PartialEq)]
pub struct StaticOAuthClientConfig {
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
}

#[cfg(feature = "mcp-http")]
#[async_trait]
pub trait ExtensionAuth: Send + Sync {
    fn credential_store(&self, name: &str) -> Box<dyn CredentialStore>;

    async fn authorize(
        &self,
        uri: &str,
        name: &str,
        static_client: Option<&StaticOAuthClientConfig>,
        challenge: Option<String>,
    ) -> Result<AuthorizationManager>;
}

#[cfg(feature = "mcp-http")]
/// Authorization for hosts without credential storage or an OAuth flow: servers
/// that require authorization fail to start.
pub struct NoExtensionAuth;

#[cfg(feature = "mcp-http")]
#[async_trait]
impl ExtensionAuth for NoExtensionAuth {
    fn credential_store(&self, _name: &str) -> Box<dyn CredentialStore> {
        Box::new(InMemoryCredentialStore::new())
    }

    async fn authorize(
        &self,
        uri: &str,
        name: &str,
        _static_client: Option<&StaticOAuthClientConfig>,
        _challenge: Option<String>,
    ) -> Result<AuthorizationManager> {
        anyhow::bail!(
            "extension '{name}' at {uri} requires authorization, which this host does not support"
        )
    }
}

/// Application services used by the extension runtime. The host owns persistence,
/// configuration, extension registration, and authorization policy.
///
/// Only `action_required` is required. The defaults suit a host that resolves its
/// own [`ExtensionSet`](super::ExtensionSet)s with [`ExtensionManager::resolve`] and
/// does not change selections at runtime: session lookup and selection changes
/// fail, configs are used as given, and authorization is unavailable.
#[async_trait]
pub trait ExtensionHost: Send + Sync {
    /// Needed by [`ExtensionManager::current_lease`], `enable`, and `disable`.
    async fn session(&self, session_id: &str) -> Result<ExtensionSession> {
        anyhow::bail!("this host does not look up extension sessions ({session_id})")
    }

    async fn select(&self, _session_id: &str, config: ExtensionConfig) -> ExtensionResult<()> {
        Err(runtime_changes_unsupported(&config.name()))
    }

    async fn disable(&self, _session_id: &str, key: &str) -> ExtensionResult<bool> {
        Err(runtime_changes_unsupported(key))
    }

    /// Resolve secrets and environment before connecting. Defaults to the config as given.
    async fn resolve_config(&self, config: ExtensionConfig) -> ExtensionResult<ExtensionConfig> {
        Ok(config)
    }

    fn extension_by_name(&self, _name: &str) -> Option<ExtensionConfig> {
        None
    }

    fn action_required(&self) -> Arc<ActionRequiredManager>;

    #[cfg(feature = "mcp-http")]
    fn auth(&self) -> Arc<dyn ExtensionAuth> {
        Arc::new(NoExtensionAuth)
    }

    fn config_generation(&self) -> u64 {
        0
    }

    fn client_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn default_timeout(&self) -> u64 {
        DEFAULT_EXTENSION_TIMEOUT
    }
    fn platform_extension(&self, _name: &str) -> Option<PlatformExtension> {
        None
    }
    fn connect_platform(
        &self,
        _name: &str,
        _manager: Weak<ExtensionManager>,
    ) -> ExtensionResult<Option<Box<dyn McpClientTrait>>> {
        Ok(None)
    }
    fn builtin_extension(&self, _name: &str) -> Option<SpawnServerFn> {
        None
    }
    fn builtin_command(&self, name: &str) -> ExtensionResult<(String, Vec<String>)> {
        Err(crate::extension::ExtensionError::ConfigError(format!(
            "No container command for builtin extension: {name}"
        )))
    }
    fn resolve_command(&self, cmd: &str) -> PathBuf {
        PathBuf::from(cmd)
    }
    fn search_path(&self) -> Result<OsString> {
        std::env::var_os("PATH").ok_or_else(|| anyhow::anyhow!("PATH is not set"))
    }
    fn trace_context(&self) -> Option<TraceContextInjector> {
        None
    }
}

fn runtime_changes_unsupported(name: &str) -> crate::extension::ExtensionError {
    crate::extension::ExtensionError::ConfigError(format!(
        "this host does not support changing extension '{name}' at runtime"
    ))
}
