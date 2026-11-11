use super::goose_host::GooseExtensionHost;
use crate::agents::extension_config::{name_to_key, ExtensionConfig, ExtensionResult};
use crate::agents::platform_extensions::PLATFORM_EXTENSIONS;
use crate::container::Container;
use async_trait::async_trait;
use goose_agent::extension_manager::host::ExtensionHost;
use goose_agent::extension_manager::{ConnectRequest, Connector, Registration};
use goose_agent::mcp_client::McpClientTrait;
use std::sync::{Arc, OnceLock};

pub(super) fn registration(
    config: ExtensionConfig,
    container: Option<Container>,
    host: Arc<GooseExtensionHost>,
) -> Registration {
    let def = match &config {
        ExtensionConfig::Builtin { name, .. } | ExtensionConfig::Platform { name, .. } => {
            PLATFORM_EXTENSIONS.get(name_to_key(name).as_str())
        }
        _ => None,
    };
    use std::hash::{Hash, Hasher};
    let mut revision = std::collections::hash_map::DefaultHasher::new();
    container.as_ref().map(|c| c.id()).hash(&mut revision);
    let revision = if def.is_some() { 0 } else { revision.finish() };
    let mut registration = Registration::new(
        config.name(),
        Arc::new(GooseConnector {
            config: config.clone(),
            container,
            host,
            resolved: OnceLock::new(),
        }),
    )
    .with_revision(revision)
    .with_metadata(serde_json::to_value(&config).expect("config serializes"));
    let available_tools = match &config {
        ExtensionConfig::Builtin {
            available_tools, ..
        }
        | ExtensionConfig::Platform {
            available_tools, ..
        }
        | ExtensionConfig::External(goose_agent::extension::ExtensionConfig::Stdio {
            available_tools,
            ..
        })
        | ExtensionConfig::External(goose_agent::extension::ExtensionConfig::StreamableHttp {
            available_tools,
            ..
        }) => available_tools.clone(),
    };
    registration = registration.with_available_tools(available_tools);
    if def.is_some() {
        registration = registration.trusted().location_independent();
    }
    if def.is_some_and(|def| def.unprefixed_tools) {
        registration = registration.unprefixed();
    }
    registration
}

struct GooseConnector {
    config: ExtensionConfig,
    container: Option<Container>,
    host: Arc<GooseExtensionHost>,
    resolved: OnceLock<ExtensionConfig>,
}
#[async_trait]
impl Connector for GooseConnector {
    async fn connect(
        &self,
        request: &ConnectRequest,
    ) -> ExtensionResult<Option<Box<dyn McpClientTrait>>> {
        let config = self
            .config
            .clone()
            .resolve(crate::config::Config::global())
            .await?;
        let resolved = self.resolved.get_or_init(|| config);
        if let ExtensionConfig::Builtin { name, .. } | ExtensionConfig::Platform { name, .. } =
            resolved
        {
            if let Some(def) = PLATFORM_EXTENSIONS.get(name_to_key(name).as_str()) {
                let mut context = self.host.context.clone();
                context.extension_manager = Some(request.manager.clone());
                return Ok((def.client_factory)(context));
            }
        }
        let working_dir = if request.working_dir.as_os_str().is_empty() {
            std::env::var("GOOSE_WORKING_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default())
        } else {
            request.working_dir.clone()
        };
        let mut context = request.connect_context(None, working_dir);
        let client: Box<dyn McpClientTrait> = match resolved {
            ExtensionConfig::Builtin { name, timeout, .. } => {
                context.timeout = std::time::Duration::from_secs(
                    timeout.unwrap_or_else(|| self.host.default_timeout()),
                );
                super::builtin::connect(name, self.container.as_ref(), context, self.host.as_ref())
                    .await?
            }
            ExtensionConfig::Platform { name, .. } => {
                super::builtin::connect(name, self.container.as_ref(), context, self.host.as_ref())
                    .await?
            }
            ExtensionConfig::External(goose_agent::extension::ExtensionConfig::Stdio {
                cmd,
                args,
                envs,
                timeout,
                cwd,
                ..
            }) => {
                context.timeout = std::time::Duration::from_secs(
                    timeout.unwrap_or_else(|| self.host.default_timeout()),
                );
                if let Some(cwd) = cwd {
                    context.working_dir = cwd.into();
                }
                goose_agent::extension_malware_check::deny_if_malicious_cmd_args(cmd, args).await?;
                let mut env = envs.get_env();
                env.insert("AGENT_SESSION_ID".to_string(), request.scope_id.clone());
                let command = self
                    .host
                    .stdio_command(cmd, args, env, self.container.as_ref())?;
                Box::new(
                    goose_agent::extension_manager::spawn_stdio(
                        command,
                        context,
                        self.host.as_ref(),
                    )
                    .await?,
                )
            }
            ExtensionConfig::External(
                goose_agent::extension::ExtensionConfig::StreamableHttp { .. },
            ) => {
                let external = resolved.external().expect("HTTP config");
                let registration = Registration::from_config(external.clone(), self.host.clone());
                return registration.connector().connect(request).await;
            }
        };
        Ok(Some(client))
    }
    async fn is_current(&self) -> bool {
        let Some(resolved) = self.resolved.get() else {
            return true;
        };
        self.config
            .clone()
            .resolve(crate::config::Config::global())
            .await
            .is_ok_and(|config| config == *resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::extension_manager::ExtensionManager;

    #[test]
    fn saved_configs_round_trip_without_changing_the_session_schema() {
        for value in [
            serde_json::json!({"type":"builtin", "name":"todo", "description":"", "display_name":null, "timeout":30, "bundled":true}),
            serde_json::json!({"type":"platform", "name":"developer", "description":"", "display_name":null, "bundled":true}),
            serde_json::json!({"type":"stdio", "name":"echo", "description":"", "cmd":"python3", "args":[], "timeout":30}),
            serde_json::json!({"type":"streamable_http", "name":"web", "description":"", "uri":"https://example.com/mcp", "timeout":30}),
        ] {
            let config: ExtensionConfig = serde_json::from_value(value).unwrap();
            let saved = serde_json::to_value(&config).unwrap();
            let manager = ExtensionManager::with_data_dir(std::env::temp_dir());
            let registration = registration(config.clone(), None, manager.host());
            assert_eq!(registration.metadata(), &saved);
            let restored: ExtensionConfig =
                serde_json::from_value(registration.metadata().clone()).unwrap();
            assert_eq!(restored, config);
            if let Some(external) = config.external() {
                assert_eq!(serde_json::to_value(external).unwrap(), saved);
            }
        }
    }

    #[tokio::test]
    async fn container_changes_external_registration_revision_but_not_in_process_revision() {
        let directory = tempfile::tempdir().unwrap();
        let manager = ExtensionManager::with_data_dir(directory.path().to_path_buf());
        let configs = vec![
            ExtensionConfig::stdio("echo", "python3", "", 30u64),
            ExtensionConfig::default(),
        ];
        let first = manager
            .extension_set(
                "scope",
                None,
                configs.clone(),
                Some(Container::new("first")),
            )
            .unwrap();
        manager.resolve(&first).await;
        let before = manager.manager.scope_lease("scope").await;
        let second = manager
            .extension_set("scope", None, configs, Some(Container::new("second")))
            .unwrap();
        let after = manager.resolve(&second).await;
        // Metadata remains the persisted config; placement identity is a revision.
        let old = before.registrations();
        let new = after.registrations();
        assert_eq!(old[0].metadata(), new[0].metadata());
        assert_ne!(old[0].revision(), new[0].revision());
        assert_eq!(old[1].revision(), new[1].revision());
    }
}
