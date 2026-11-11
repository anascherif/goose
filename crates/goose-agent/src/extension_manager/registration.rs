//! What the runtime selects: a named [`Registration`] whose [`Connector`] opens the
//! MCP client. `ExtensionConfig` selections become registrations with a built-in
//! connector; hosts can register their own transports the same way.

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;

#[cfg(feature = "mcp-stdio")]
use super::stdio;
#[cfg(feature = "mcp-http")]
use super::streamable_http;
use super::{host::ExtensionHost, ClientContext, ExtensionManager};
use crate::extension::{name_to_key, ExtensionConfig, ExtensionResult};
use crate::mcp_client::{ConnectContext, McpClientTrait};

/// Opens the MCP client for one registration.
#[async_trait]
pub trait Connector: Send + Sync {
    /// `Ok(None)` declines: the extension is unavailable here and contributes no
    /// tools, without counting as a failure.
    async fn connect(
        &self,
        request: &ConnectRequest,
    ) -> ExtensionResult<Option<Box<dyn McpClientTrait>>>;

    /// Whether a running connection still reflects this connector's inputs. Asked
    /// only after the host's `config_generation` changes; `false` restarts the
    /// extension on the next lease.
    async fn is_current(&self) -> bool {
        true
    }
}

/// Where and for whom a registration connects.
pub struct ConnectRequest {
    pub scope_id: String,
    /// The scope's working directory (or the process's, if it has none).
    pub working_dir: PathBuf,
    pub client: Arc<ClientContext>,
    tools_version: Arc<AtomicU64>,
    host: Arc<dyn ExtensionHost>,
    pub manager: Weak<ExtensionManager>,
}

impl ConnectRequest {
    pub(super) fn new(
        scope_id: &str,
        working_dir: PathBuf,
        client: Arc<ClientContext>,
        manager: &Arc<ExtensionManager>,
    ) -> Self {
        Self {
            scope_id: scope_id.to_string(),
            working_dir,
            client,
            tools_version: Arc::new(AtomicU64::new(0)),
            host: Arc::clone(&manager.host),
            manager: Arc::downgrade(manager),
        }
    }

    /// Increment on `notifications/tools/list_changed`: a cached tool list is only
    /// reused while this is unchanged.
    pub fn tools_version(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.tools_version)
    }

    pub fn connect_context(&self, timeout: Option<u64>, working_dir: PathBuf) -> ConnectContext {
        ConnectContext {
            timeout: Duration::from_secs(timeout.unwrap_or_else(|| self.host.default_timeout())),
            client_name: self.client.name.clone(),
            client_version: self.host.client_version(),
            capabilities: self.client.capabilities.clone(),
            working_dir,

            action_required: self.host.action_required(),
            tools_version: Arc::clone(&self.tools_version),
            trace_context: self.host.trace_context(),
        }
    }
}

/// One selectable extension. A lease reuses a running extension only for an
/// equivalent registration: bump the revision when its connector's inputs change.
#[derive(Clone)]
pub struct Registration {
    pub(super) key: String,
    pub(super) name: String,
    pub(super) connector: Arc<dyn Connector>,
    revision: u64,
    available_tools: Vec<String>,
    pub(super) unprefixed: bool,
    /// Trusted in-process connectors get the MOIM and take the working directory
    /// from each call rather than restarting when it changes.
    pub(super) trusted: bool,
    pub(super) placed: bool,
    metadata: serde_json::Value,
    pub(super) config: Option<ExtensionConfig>,
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("name", &self.name)
            .field("revision", &self.revision)
            .field("available_tools", &self.available_tools)
            .field("unprefixed", &self.unprefixed)
            .finish_non_exhaustive()
    }
}

impl Registration {
    pub fn new(name: impl Into<String>, connector: Arc<dyn Connector>) -> Self {
        let name = name.into();
        Self {
            key: name_to_key(&name),
            name,
            connector,
            revision: 0,
            available_tools: Vec::new(),
            unprefixed: false,
            trusted: false,
            placed: true,
            metadata: serde_json::Value::Null,
            config: None,
        }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn with_revision(mut self, revision: u64) -> Self {
        self.revision = revision;
        self
    }

    /// Expose only these tools (by server name). Empty exposes all.
    pub fn with_available_tools(mut self, tools: Vec<String>) -> Self {
        self.available_tools = tools;
        self
    }

    /// Expose tools under their server names instead of `{name}__{tool}`.
    pub fn unprefixed(mut self) -> Self {
        self.unprefixed = true;
        self
    }

    pub fn connector(&self) -> &Arc<dyn Connector> {
        &self.connector
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn from_config(config: ExtensionConfig, host: Arc<dyn ExtensionHost>) -> Self {
        let available_tools = match &config {
            ExtensionConfig::StreamableHttp {
                available_tools, ..
            }
            | ExtensionConfig::Stdio {
                available_tools, ..
            } => available_tools.clone(),
        };
        Self {
            key: config.key(),
            name: config.name(),
            connector: Arc::new(ConfigConnector {
                config: config.clone(),
                resolved: OnceLock::new(),
                host,
            }),
            revision: 0,
            available_tools,
            unprefixed: false,
            trusted: false,
            placed: true,
            metadata: serde_json::Value::Null,
            config: Some(config),
        }
    }

    /// Mark an in-process connector as trusted to emit host-reserved metadata.
    pub fn trusted(mut self) -> Self {
        self.trusted = true;
        self
    }

    /// Do not restart this connector when the scope's working directory changes.
    pub fn location_independent(mut self) -> Self {
        self.placed = false;
        self
    }

    /// Opaque host data; participates in registration equivalence.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
    pub fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    pub(super) fn is_tool_available(&self, tool: &str) -> bool {
        self.available_tools.is_empty() || self.available_tools.iter().any(|name| name == tool)
    }

    /// Whether a slot started for `self` can serve `other`. Config-backed
    /// registrations compare configs; host registrations compare revisions.
    pub(super) fn is_equivalent(&self, other: &Registration) -> bool {
        self.key == other.key
            && self.revision == other.revision
            && self.available_tools == other.available_tools
            && self.unprefixed == other.unprefixed
            && self.trusted == other.trusted
            && self.placed == other.placed
            && self.metadata == other.metadata
            && self.config == other.config
    }
}

/// Connects an `ExtensionConfig` after the host resolves its secrets.
struct ConfigConnector {
    config: ExtensionConfig,
    resolved: OnceLock<ExtensionConfig>,
    host: Arc<dyn ExtensionHost>,
}

#[async_trait]
impl Connector for ConfigConnector {
    async fn connect(
        &self,
        request: &ConnectRequest,
    ) -> ExtensionResult<Option<Box<dyn McpClientTrait>>> {
        let _ = request;
        let resolved = self.host.resolve_config(self.config.clone()).await?;
        let resolved = self.resolved.get_or_init(|| resolved);
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let working_dir = match resolved {
            ExtensionConfig::Stdio { cwd: Some(cwd), .. } => PathBuf::from(cwd),
            _ if request.working_dir.as_os_str().is_empty() => {
                std::env::current_dir().unwrap_or_default()
            }
            _ => request.working_dir.clone(),
        };
        #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
        let host = self.host.as_ref();
        match resolved {
            #[cfg(feature = "mcp-http")]
            ExtensionConfig::StreamableHttp {
                uri,
                timeout,
                headers,
                name,
                envs,
                socket,
                client_id,
                client_secret_key,
                scopes,
                ..
            } => {
                let static_oauth_client = streamable_http::resolve_static_oauth_client(
                    client_id.as_deref(),
                    client_secret_key.as_deref(),
                    scopes,
                    &envs.get_env(),
                )?;
                let params = streamable_http::ConnectParams {
                    uri: uri.clone(),
                    name: name.clone(),
                    headers: headers.clone(),
                    static_oauth_client,
                    ctx: request.connect_context(*timeout, working_dir),
                    auth: host.auth(),
                };
                let client = streamable_http::connect(
                    params,
                    socket.as_deref(),
                    host.auth().credential_store(name),
                )
                .await?;
                Ok(Some(client))
            }
            #[cfg(not(feature = "mcp-http"))]
            ExtensionConfig::StreamableHttp { .. } => {
                Err(transport_disabled("streamable_http", "mcp-http"))
            }
            #[cfg(feature = "mcp-stdio")]
            ExtensionConfig::Stdio {
                cmd,
                args,
                envs,
                timeout,
                ..
            } => {
                let mut envs = envs.get_env();
                envs.insert("AGENT_SESSION_ID".to_string(), request.scope_id.clone());
                let client = Box::new(
                    stdio::connect(
                        cmd,
                        args,
                        envs,
                        request.connect_context(*timeout, working_dir),
                        host,
                    )
                    .await?,
                );
                Ok(Some(client as Box<dyn McpClientTrait>))
            }
            #[cfg(not(feature = "mcp-stdio"))]
            ExtensionConfig::Stdio { .. } => Err(transport_disabled("stdio", "mcp-stdio")),
        }
    }

    /// Secrets and environment may have changed since the extension started.
    async fn is_current(&self) -> bool {
        let Some(resolved) = self.resolved.get() else {
            return true;
        };
        self.host
            .resolve_config(self.config.clone())
            .await
            .is_ok_and(|config| config == *resolved)
    }
}

#[cfg(not(all(feature = "mcp-stdio", feature = "mcp-http")))]
fn transport_disabled(kind: &str, feature: &str) -> crate::extension::ExtensionError {
    crate::extension::ExtensionError::ConfigError(format!(
        "{kind} extensions require goose-agent's `{feature}` feature"
    ))
}
