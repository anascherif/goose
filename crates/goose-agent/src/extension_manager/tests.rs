use super::*;
use crate::extension::check_unique_keys;
use crate::extension_manager::host::PlatformExtension;
#[cfg(feature = "mcp-http")]
use crate::extension_manager::host::{ExtensionAuth, StaticOAuthClientConfig};
use crate::tool_execution::ToolCallContext;
use async_trait::async_trait;
use rmcp::model::{InitializeResult, JsonObject, ListToolsResult};
#[cfg(feature = "mcp-http")]
use rmcp::transport::auth::{CredentialStore, InMemoryCredentialStore};
#[cfg(feature = "mcp-http")]
use rmcp::transport::AuthorizationManager;
use rmcp::ServiceError as Error;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Semaphore;

struct TestHost {
    session: Mutex<ExtensionSession>,
    action_required: Arc<ActionRequiredManager>,
}

impl TestHost {
    fn new() -> Self {
        Self {
            session: Mutex::new(ExtensionSession {
                id: "session".to_string(),
                working_dir: PathBuf::new(),
                container: None,
                extensions: vec![],
                provider_runs_tool_loop: false,
            }),
            action_required: Arc::new(ActionRequiredManager::new()),
        }
    }
}

#[async_trait]
impl ExtensionHost for TestHost {
    async fn session(&self, session_id: &str) -> Result<ExtensionSession> {
        let session = self.session.lock().await;
        anyhow::ensure!(session.id == session_id, "unknown session");
        Ok(session.clone())
    }

    async fn select(&self, _session_id: &str, config: ExtensionConfig) -> ExtensionResult<()> {
        let mut session = self.session.lock().await;
        match session
            .extensions
            .iter_mut()
            .find(|selected| selected.key() == config.key())
        {
            Some(selected) => *selected = config,
            None => session.extensions.push(config),
        }
        check_unique_keys(&session.extensions)
            .map_err(|error| ExtensionError::ConfigError(error.to_string()))
    }

    async fn disable(&self, _session_id: &str, key: &str) -> ExtensionResult<bool> {
        let mut session = self.session.lock().await;
        let selected = session.extensions.len();
        session.extensions.retain(|config| config.key() != key);
        Ok(session.extensions.len() != selected)
    }

    async fn resolve_config(&self, config: ExtensionConfig) -> ExtensionResult<ExtensionConfig> {
        Ok(config)
    }

    fn extension_by_name(&self, _name: &str) -> Option<ExtensionConfig> {
        None
    }
    fn action_required(&self) -> Arc<ActionRequiredManager> {
        Arc::clone(&self.action_required)
    }
    #[cfg(feature = "mcp-http")]
    fn auth(&self) -> Arc<dyn ExtensionAuth> {
        Arc::new(NoAuth)
    }

    fn platform_extension(&self, name: &str) -> Option<PlatformExtension> {
        (name == "developer").then_some(PlatformExtension {
            unprefixed_tools: true,
        })
    }
}

#[cfg(feature = "mcp-http")]
struct NoAuth;

#[cfg(feature = "mcp-http")]
#[async_trait]
impl ExtensionAuth for NoAuth {
    fn credential_store(&self, _name: &str) -> Box<dyn CredentialStore> {
        Box::new(InMemoryCredentialStore::new())
    }

    async fn authorize(
        &self,
        _uri: &str,
        _name: &str,
        _static_client: Option<&StaticOAuthClientConfig>,
        _challenge: Option<String>,
    ) -> Result<AuthorizationManager> {
        anyhow::bail!("OAuth is not available in this test")
    }
}

fn manager() -> Arc<ExtensionManager> {
    manager_with(StartupPolicy::default())
}

fn manager_with(startup: StartupPolicy) -> Arc<ExtensionManager> {
    Arc::new(
        ExtensionManager::with_host(
            Arc::new(TestHost::new()),
            ClientContext {
                name: "test".to_string(),
                capabilities: GooseMcpClientCapabilities {
                    mcpui: false,
                    host_info: None,
                    elicitation_handler: None,
                    action_required_elicitation: true,
                    protocol_version: None,
                },
            },
        )
        .with_startup_policy(startup),
    )
}

fn builtin_config(name: &str) -> ExtensionConfig {
    ExtensionConfig::Builtin {
        name: name.to_string(),
        display_name: None,
        description: String::new(),
        timeout: None,
        bundled: None,
        available_tools: vec![],
    }
}

struct BlockingToolsClient {
    calls: AtomicUsize,
    first_fetch_started: Semaphore,
    release_first_fetch: Semaphore,
}

#[async_trait::async_trait]
impl McpClientTrait for BlockingToolsClient {
    async fn list_tools(
        &self,
        _session_id: &str,
        _next_cursor: Option<String>,
        _cancel_token: CancellationToken,
    ) -> Result<ListToolsResult, Error> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let name = if call == 0 { "old" } else { "new" };

        if call == 0 {
            self.first_fetch_started.add_permits(1);
            let _permit = self.release_first_fetch.acquire().await.unwrap();
        }

        Ok(ListToolsResult {
            tools: vec![Tool::new(
                name,
                format!("{name} tool list"),
                Arc::new(JsonObject::new()),
            )],
            next_cursor: None,
            meta: None,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        _ctx: &ToolCallContext,
        _name: &str,
        _arguments: Option<JsonObject>,
        _cancel_token: CancellationToken,
    ) -> Result<CallToolResult, Error> {
        Ok(CallToolResult::success(vec![]))
    }

    fn get_info(&self) -> Option<&InitializeResult> {
        None
    }
}

#[tokio::test]
async fn tool_list_changed_during_fetch_prevents_stale_cache() {
    let extension_manager = manager();
    let tools_client = Arc::new(BlockingToolsClient {
        calls: AtomicUsize::new(0),
        first_fetch_started: Semaphore::new(0),
        release_first_fetch: Semaphore::new(0),
    });
    extension_manager
        .add_client(
            "session",
            builtin_config("dynamic"),
            tools_client.clone(),
            None,
        )
        .await;
    let slot = extension_manager.scopes.lock().await["session"]["dynamic"].clone();
    let tools_version = slot.start().await.unwrap().tools_version.clone();

    let manager = extension_manager;
    let first_fetch = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.scope_lease("session").await.tools().await })
    };

    let _started = tools_client.first_fetch_started.acquire().await.unwrap();
    tools_version.fetch_add(1, Ordering::SeqCst);
    tools_client.release_first_fetch.add_permits(1);

    let stale_result = first_fetch.await.unwrap().unwrap();
    assert!(stale_result.iter().any(|tool| tool.name == "dynamic__old"));

    let refreshed = manager.scope_lease("session").await.tools().await.unwrap();
    assert!(refreshed.iter().any(|tool| tool.name == "dynamic__new"));
    assert_eq!(tools_client.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn test_recover_mangled_tool_name() {
    let tools = [("developer__shell", None), ("platform__search", None)];
    assert_eq!(
        recover_mangled_tool_name("developer.shell", tools.iter().copied()).as_deref(),
        Some("developer__shell")
    );
    assert_eq!(
        recover_mangled_tool_name("functions.developer__shell", tools.iter().copied()).as_deref(),
        Some("developer__shell")
    );
    assert_eq!(
        recover_mangled_tool_name("functions.developer.shell", tools.iter().copied()).as_deref(),
        Some("developer__shell")
    );
    assert_eq!(
        recover_mangled_tool_name("developer shell", tools.iter().copied()),
        None
    );
    assert_eq!(
        recover_mangled_tool_name("developer__shell!", tools.iter().copied()),
        None
    );
    assert_eq!(
        recover_mangled_tool_name("nonexistent.tool", tools.iter().copied()),
        None
    );

    let dotted_tool = [("dotted__db.query", None)];
    assert_eq!(
        recover_mangled_tool_name("dotted.db.query", dotted_tool.iter().copied()).as_deref(),
        Some("dotted__db.query")
    );
}

#[test]
fn test_recover_mangled_tool_name_unprefixed_extension() {
    // Platform extensions with unprefixed_tools=true (e.g. "developer")
    // advertise tools with no "__" prefix at all; the owner lives only in
    // metadata. GLM's documented "developer.shell" reproduction (#9486)
    // and emulated "developer__shell" calls must recover via the owner,
    // not the tool's own (absent) prefix.
    let tools = [("shell", Some("developer")), ("write", Some("developer"))];
    assert_eq!(
        recover_mangled_tool_name("developer.shell", tools.iter().copied()).as_deref(),
        Some("shell")
    );
    assert_eq!(
        recover_mangled_tool_name("developer__shell", tools.iter().copied()).as_deref(),
        Some("shell")
    );
    assert_eq!(
        recover_mangled_tool_name("functions.developer.shell", tools.iter().copied()).as_deref(),
        Some("shell")
    );

    // Wrong owner must not match.
    assert_eq!(
        recover_mangled_tool_name("other_extension.shell", tools.iter().copied()),
        None
    );

    // Ambiguity across two different unprefixed extensions that both own
    // a tool matching the same mangled input must refuse, not guess.
    let ambiguous = [("shell", Some("dev_a")), ("shell", Some("dev_b"))];
    assert_eq!(
        recover_mangled_tool_name("dev_a.shell", ambiguous.iter().copied()).as_deref(),
        Some("shell")
    );
}

#[test]
fn test_recover_mangled_tool_name_non_extension_manager_tools() {
    // recipe__final_output and platform__manage_schedule are appended by
    // Agent::list_tools outside the extension manager (see #9486); they
    // use the same "__" convention, so no owner metadata is needed.
    let tools = [
        ("recipe__final_output", None),
        ("platform__manage_schedule", None),
    ];
    assert_eq!(
        recover_mangled_tool_name("recipe.final_output", tools.iter().copied()).as_deref(),
        Some("recipe__final_output")
    );
    assert_eq!(
        recover_mangled_tool_name("platform.manage_schedule", tools.iter().copied()).as_deref(),
        Some("platform__manage_schedule")
    );
}

#[tokio::test]
async fn a_new_working_dir_restarts_only_extensions_that_run_in_it() {
    let manager = manager();
    let developer = builtin_config("developer");
    let external = builtin_config("external");
    let builtin = builtin_config("builtin");
    manager
        .add_client(
            "session",
            external.clone(),
            Arc::new(BlockingToolsClient {
                calls: AtomicUsize::new(1),
                first_fetch_started: Semaphore::new(0),
                release_first_fetch: Semaphore::new(0),
            }),
            None,
        )
        .await;
    let extensions = vec![developer, external, builtin];
    manager
        .resolve(
            &ExtensionSet::new("session", Some(PathBuf::from("old")), extensions.clone()).unwrap(),
        )
        .await;
    let before = manager.scopes.lock().await["session"].clone();

    let new_working_dir = PathBuf::from("new");
    let lease = manager
        .resolve(&ExtensionSet::new("session", Some(new_working_dir.clone()), extensions).unwrap())
        .await;
    let after = manager.scopes.lock().await["session"].clone();
    for key in ["developer", "external"] {
        assert!(
            Arc::ptr_eq(&before[key], &after[key]),
            "{key} was restarted"
        );
    }
    assert!(!Arc::ptr_eq(&before["builtin"], &after["builtin"]));
    assert_eq!(lease.working_dir(), Some(new_working_dir.as_path()));
}

#[tokio::test]
async fn host_backed_selection_keeps_held_leases_unchanged() {
    let manager = manager();
    manager
        .add_client(
            "session",
            builtin_config("dynamic"),
            Arc::new(BlockingToolsClient {
                calls: AtomicUsize::new(1),
                first_fetch_started: Semaphore::new(0),
                release_first_fetch: Semaphore::new(0),
            }),
            None,
        )
        .await;
    let before = manager.current_lease("session").await.unwrap();
    assert_eq!(
        manager.list_extensions("session").await.unwrap(),
        vec!["dynamic"]
    );

    assert!(manager.disable("session", "dynamic").await.unwrap());
    let after = manager.current_lease("session").await.unwrap();
    assert!(after.tools().await.unwrap().is_empty());
    assert_eq!(before.tools().await.unwrap()[0].name, "dynamic__new");
    let result = before
        .call(
            rmcp::model::CallToolRequestParams::new("dynamic__new"),
            CallRequest::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
        .result
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));
}

/// A host that resolves its own extension sets needs only `action_required`.
struct MinimalHost(Arc<ActionRequiredManager>);

impl ExtensionHost for MinimalHost {
    fn action_required(&self) -> Arc<ActionRequiredManager> {
        Arc::clone(&self.0)
    }
}

#[tokio::test]
async fn minimal_host_defaults_reject_runtime_changes_and_authorization() {
    let host = Arc::new(MinimalHost(Arc::new(ActionRequiredManager::new())));
    let manager = Arc::new(ExtensionManager::with_host(
        host.clone(),
        ClientContext {
            name: "minimal".to_string(),
            capabilities: Default::default(),
        },
    ));
    let lease = manager
        .resolve(&ExtensionSet::new("scope", None, vec![]).unwrap())
        .await;
    assert!(lease.tools().await.unwrap().is_empty());

    let config = ExtensionConfig::stdio("server", "server", "", 5u64);
    assert!(manager.current_lease("scope").await.is_err());
    assert!(manager.enable("scope", config.clone()).await.is_err());
    assert!(host.select("scope", config.clone()).await.is_err());
    assert!(host.disable("scope", "server").await.is_err());
    assert!(host.extension_by_name("server").is_none());
    assert_eq!(host.resolve_config(config.clone()).await.unwrap(), config);
    #[cfg(feature = "mcp-http")]
    {
        let error = host
            .auth()
            .authorize("https://example.com/mcp", "server", None, None)
            .await
            .err()
            .expect("authorization is unavailable");
        assert!(error.to_string().contains("requires authorization"));
    }
}

/// Lists fixed tools, or fails discovery when `tools` is `None`.
struct StaticToolsClient {
    tools: Option<Vec<&'static str>>,
}

#[async_trait::async_trait]
impl McpClientTrait for StaticToolsClient {
    async fn list_tools(
        &self,
        _session_id: &str,
        _next_cursor: Option<String>,
        _cancel_token: CancellationToken,
    ) -> Result<ListToolsResult, Error> {
        let Some(tools) = &self.tools else {
            return Err(Error::UnexpectedResponse);
        };
        Ok(ListToolsResult {
            tools: tools
                .iter()
                .map(|name| Tool::new(*name, "", Arc::new(JsonObject::new())))
                .collect(),
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        _ctx: &ToolCallContext,
        _name: &str,
        _arguments: Option<JsonObject>,
        _cancel_token: CancellationToken,
    ) -> Result<CallToolResult, Error> {
        Ok(CallToolResult::success(vec![]))
    }

    fn get_info(&self) -> Option<&InitializeResult> {
        None
    }
}

async fn add_static(manager: &ExtensionManager, name: &str, tools: Option<Vec<&'static str>>) {
    manager
        .add_client(
            "session",
            builtin_config(name),
            Arc::new(StaticToolsClient { tools }),
            None,
        )
        .await;
}

fn tool_names(tools: Vec<Tool>) -> Vec<String> {
    tools
        .into_iter()
        .map(|tool| tool.name.to_string())
        .collect()
}

#[tokio::test]
async fn strict_startup_fails_the_catalog_when_discovery_fails() {
    let manager = manager();
    add_static(&manager, "good", Some(vec!["tool"])).await;
    add_static(&manager, "broken", None).await;
    let lease = manager.scope_lease("session").await;

    let error = lease.tools().await.unwrap_err().to_string();
    assert!(error.contains("broken failed to list tools"), "{error}");
    assert!(lease.ready().await.is_err());
}

#[tokio::test]
async fn best_effort_startup_omits_failed_extensions() {
    let manager = manager_with(StartupPolicy::BestEffort);
    add_static(&manager, "good", Some(vec!["tool"])).await;
    add_static(&manager, "broken", None).await;
    let lease = manager.scope_lease("session").await;

    assert_eq!(tool_names(lease.tools().await.unwrap()), ["good__tool"]);
    assert_eq!(lease.ready().await.unwrap().len(), 2);
}

#[tokio::test]
async fn strict_startup_fails_the_catalog_when_an_extension_fails_to_start() {
    let config = ExtensionConfig::stdio("missing", "/nonexistent/mcp-server", "", 5u64);
    for (startup, fails) in [
        (StartupPolicy::Strict, true),
        (StartupPolicy::BestEffort, false),
    ] {
        let manager = manager_with(startup);
        let lease = manager
            .resolve(&ExtensionSet::new("scope", None, vec![config.clone()]).unwrap())
            .await;
        let results = lease.start().await;
        assert!(results[0].error.is_some());
        assert_eq!(lease.tools().await.is_err(), fails, "{startup:?}");
    }
}

#[tokio::test]
async fn colliding_tool_names_keep_the_first_extension() {
    // `developer` is unprefixed, so its `other__tool` collides with `other`'s `tool`.
    let manager = manager();
    add_static(&manager, "developer", Some(vec!["other__tool"])).await;
    add_static(&manager, "other", Some(vec!["tool"])).await;
    let lease = manager.scope_lease("session").await;
    let tools = lease.tools().await.unwrap();
    assert_eq!(tool_names(tools.clone()), ["other__tool"]);
    assert_eq!(get_tool_owner(&tools[0]).as_deref(), Some("developer"));
}

/// Hands out `StaticToolsClient`s, counting connections; declines when `tools` is `None`.
struct StaticConnector {
    tools: Option<Vec<&'static str>>,
    connects: AtomicUsize,
}

impl StaticConnector {
    fn new(tools: Option<Vec<&'static str>>) -> Arc<Self> {
        Arc::new(Self {
            tools,
            connects: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl Connector for StaticConnector {
    async fn connect(
        &self,
        request: &ConnectRequest,
    ) -> ExtensionResult<Option<Box<dyn McpClientTrait>>> {
        assert_eq!(request.scope_id, "scope");
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(self.tools.clone().map(|tools| {
            Box::new(StaticToolsClient { tools: Some(tools) }) as Box<dyn McpClientTrait>
        }))
    }
}

fn registration_set(registrations: Vec<Registration>) -> ExtensionSet {
    registrations
        .into_iter()
        .try_fold(
            ExtensionSet::new("scope", None, vec![]).unwrap(),
            ExtensionSet::with_registration,
        )
        .unwrap()
}

#[tokio::test]
async fn host_registrations_connect_through_their_connector() {
    let manager = manager();
    let connector = StaticConnector::new(Some(vec!["echo", "hidden"]));
    let registration = Registration::new("custom", connector.clone())
        .with_available_tools(vec!["echo".to_string()]);
    let lease = manager
        .resolve(&registration_set(vec![registration.clone()]))
        .await;

    assert_eq!(tool_names(lease.tools().await.unwrap()), ["custom__echo"]);
    let result = lease
        .call(
            rmcp::model::CallToolRequestParams::new("custom__echo"),
            CallRequest::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap()
        .result
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));

    // The same revision reuses the connection; a new one reconnects.
    let same = manager
        .resolve(&registration_set(vec![registration.clone()]))
        .await;
    same.tools().await.unwrap();
    assert_eq!(connector.connects.load(Ordering::SeqCst), 1);
    let bumped = manager
        .resolve(&registration_set(vec![registration.with_revision(1)]))
        .await;
    bumped.tools().await.unwrap();
    assert_eq!(connector.connects.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn declining_connectors_contribute_no_tools_without_failing() {
    let manager = manager();
    let lease = manager
        .resolve(&registration_set(vec![
            Registration::new("declined", StaticConnector::new(None)),
            Registration::new("first", StaticConnector::new(Some(vec!["tool"]))).unprefixed(),
        ]))
        .await;
    assert_eq!(tool_names(lease.tools().await.unwrap()), ["tool"]);
    assert!(lease.start().await.iter().all(|result| result.success));
}

#[test]
fn registration_keys_must_be_unique_within_a_set() {
    let set = ExtensionSet::new("scope", None, vec![builtin_config("dup")]).unwrap();
    assert!(set
        .with_registration(Registration::new("dup", StaticConnector::new(None)))
        .is_err());
}

struct ScopeResolver(Arc<ExtensionManager>);

#[async_trait]
impl LeaseResolver<()> for ScopeResolver {
    async fn resolve(&self, _session: &()) -> Result<ExtensionLease> {
        Ok(self.0.scope_lease("session").await)
    }
}

fn emitter() -> (
    crate::operation::Emitter,
    tokio::sync::mpsc::UnboundedReceiver<crate::events::AgentEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (
        crate::operation::Emitter::new(tx, CancellationToken::new()),
        rx,
    )
}

fn turn_requesting(names: &[&str]) -> goose_provider_types::conversation::Conversation {
    use goose_provider_types::conversation::message::Message;
    let request = names.iter().fold(Message::assistant(), |message, name| {
        message.with_tool_request(
            format!("call-{name}"),
            Ok(rmcp::model::CallToolRequestParams::new(name.to_string())),
        )
    });
    goose_provider_types::conversation::Conversation::new_unvalidated([
        Message::user().with_text("go"),
        request,
    ])
}

fn response_texts(
    result: crate::operation::OperationResult<goose_provider_types::conversation::message::Message>,
) -> Vec<(String, String)> {
    let crate::operation::OperationResult::Applied(step) = result else {
        panic!("operation did not apply");
    };
    step.effects
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| content.as_tool_response())
        .map(|response| {
            let text = response
                .tool_result
                .as_ref()
                .unwrap()
                .content
                .first()
                .and_then(|content| content.as_text())
                .map(|text| text.text.clone())
                .unwrap_or_default();
            (response.id.clone(), text)
        })
        .collect()
}

use crate::operation::Operation;
use std::time::Duration;

type Op = ExtensionOperation<()>;
type Msg = goose_provider_types::conversation::message::Message;

#[tokio::test]
async fn extension_operation_runs_calls_to_tools_its_inference_advertised() {
    let manager = manager();
    add_static(&manager, "first", Some(vec!["a"])).await;
    add_static(&manager, "second", Some(vec!["b"])).await;
    let operation = Op::new(Arc::new(ScopeResolver(manager)));
    let (emit, _events) = emitter();

    let tools = Operation::<(), Msg>::inference_tools(&operation, &())
        .await
        .unwrap();
    assert_eq!(tool_names(tools), ["first__a", "second__b"]);
    let result = Operation::<(), Msg>::run(
        &operation,
        &(),
        &turn_requesting(&["first__a", "second__b", "shell"]),
        &emit,
    )
    .await
    .unwrap();
    // `shell` belongs to another operation.
    let responses = response_texts(result);
    let ids: Vec<_> = responses.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["call-first__a", "call-second__b"]);
    assert!(responses
        .iter()
        .all(|(_, text)| text != EXPIRED_TOOL_REQUEST));
}

#[tokio::test]
async fn extension_operation_expires_requests_no_inference_advertised() {
    let manager = manager();
    add_static(&manager, "first", Some(vec!["a"])).await;
    let operation = Op::new(Arc::new(ScopeResolver(manager)));
    let (emit, _events) = emitter();

    let result = Operation::<(), Msg>::run(&operation, &(), &turn_requesting(&["first__a"]), &emit)
        .await
        .unwrap();
    assert_eq!(
        response_texts(result),
        [(
            "call-first__a".to_string(),
            EXPIRED_TOOL_REQUEST.to_string()
        )]
    );
}

fn notification() -> rmcp::model::ServerNotification {
    use rmcp::model::{Notification, NumberOrString, ProgressNotificationParam, ProgressToken};
    rmcp::model::ServerNotification::ProgressNotification(Notification::new(
        ProgressNotificationParam::new(ProgressToken(NumberOrString::String(Arc::from("t"))), 1.0),
    ))
}

#[tokio::test]
async fn tool_call_batches_run_concurrently_and_keep_notifications_sent_with_results() {
    for _ in 0..20 {
        let batch = ToolCallBatch::default();
        let (emit, mut events) = emitter();
        let (release_first, released) = tokio::sync::oneshot::channel::<()>();
        let (notify, notifications) = tokio::sync::mpsc::channel(4);
        // The notification is queued before the result is ready, as when a client
        // delivers it on its own task.
        notify.try_send(notification()).unwrap();
        let first = crate::tool_execution::ToolCallResult {
            result: Box::new(
                async move {
                    released.await.unwrap();
                    Ok(CallToolResult::success(vec![]))
                }
                .boxed(),
            ),
            notification_stream: Some(Box::new(tokio_stream::wrappers::ReceiverStream::new(
                notifications,
            ))),
            action_required_stream: None,
        };
        let second = crate::tool_execution::ToolCallResult {
            result: Box::new(
                async move {
                    release_first.send(()).unwrap();
                    Ok(CallToolResult::success(vec![]))
                }
                .boxed(),
            ),
            notification_stream: None,
            action_required_stream: None,
        };
        let calls = ["first", "second"].into_iter().zip([first, second]);
        tokio::time::timeout(
            Duration::from_secs(5),
            batch.run(
                calls
                    .map(|(id, call)| PendingCall {
                        request_id: id.to_string(),
                        metadata: None,
                        call,
                    })
                    .collect(),
                &emit,
                &|_, _| {},
            ),
        )
        .await
        .expect("calls run concurrently");

        let (_, response) = batch.take();
        assert_eq!(response.unwrap().get_tool_response_ids().len(), 2);
        assert!(matches!(
            events.try_recv(),
            Ok(crate::events::AgentEvent::McpNotification((id, _))) if id == "first"
        ));
        drop(notify);
    }
}
