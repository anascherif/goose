use super::host::ExtensionHost;
#[cfg(feature = "mcp-stdio")]
use super::stdio;
use crate::container::Container;
use crate::extension::name_to_key;
use crate::extension::{ExtensionError, ExtensionResult};
use crate::mcp_client::{ConnectContext, McpClient, McpClientTrait};

pub(super) async fn connect(
    name: &str,
    container: Option<&Container>,
    ctx: ConnectContext,
    host: &dyn ExtensionHost,
) -> ExtensionResult<Box<dyn McpClientTrait>> {
    let key = name_to_key(name);
    let extension_fn = host
        .builtin_extension(&key)
        .ok_or_else(|| ExtensionError::ConfigError(format!("Unknown extension: {}", name)))?;

    #[cfg(not(feature = "mcp-stdio"))]
    if container.is_some() {
        return Err(ExtensionError::ConfigError(format!(
            "builtin extension {name} in a container requires goose-agent's `mcp-stdio` feature"
        )));
    }
    #[cfg(feature = "mcp-stdio")]
    if let Some(container) = container {
        let mut ctx = ctx;
        ctx.docker_container = Some(container.id().to_string());
        tracing::info!(
            container = %container.id(),
            builtin = %name,
            "Starting builtin extension inside Docker container"
        );
        let (program, args) = host.builtin_command(&key)?;
        let command = stdio::docker_exec(container, Default::default(), &program, &args);
        return Ok(Box::new(stdio::spawn(command, ctx, host).await?));
    }

    let (server_read, client_write) = tokio::io::duplex(65536);
    let (client_read, server_write) = tokio::io::duplex(65536);
    extension_fn(server_read, server_write);
    Ok(Box::new(
        McpClient::connect((client_read, client_write), ctx).await?,
    ))
}
