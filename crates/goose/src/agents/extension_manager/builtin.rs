use super::goose_host::GooseExtensionHost;
use goose_agent::container::Container;
use goose_agent::extension::name_to_key;
use goose_agent::extension::{ExtensionError, ExtensionResult};
use goose_agent::extension_manager::host::ExtensionHost;
use goose_agent::mcp_client::{ConnectContext, McpClient, McpClientTrait};

pub(super) async fn connect(
    name: &str,
    container: Option<&Container>,
    ctx: ConnectContext,
    host: &GooseExtensionHost,
) -> ExtensionResult<Box<dyn McpClientTrait>> {
    let key = name_to_key(name);
    let extension_fn = crate::builtin_extension::get_builtin_extension(&key)
        .ok_or_else(|| ExtensionError::ConfigError(format!("Unknown extension: {}", name)))?;

    if let Some(container) = container {
        let mut ctx = ctx;
        ctx.docker_container = Some(container.id().to_string());
        tracing::info!(
            container = %container.id(),
            builtin = %name,
            "Starting builtin extension inside Docker container"
        );
        let args = vec!["mcp".to_string(), key];
        let command = host.stdio_command("goose", &args, Default::default(), Some(container))?;
        return Ok(Box::new(
            goose_agent::extension_manager::spawn_stdio(command, ctx, host).await?,
        ));
    }

    let (server_read, client_write) = tokio::io::duplex(65536);
    let (client_read, server_write) = tokio::io::duplex(65536);
    extension_fn(server_read, server_write);
    Ok(Box::new(
        McpClient::connect((client_read, client_write), ctx).await?,
    ))
}
