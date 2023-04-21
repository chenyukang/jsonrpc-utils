//! Example HTTP, WebSocket and TCP JSON-RPC server.

use std::{sync::Arc, time::Duration};

use axum::{error_handling::HandleErrorLayer, http::StatusCode, BoxError};
use jsonrpc_core::{MetaIoHandler, Params};
use jsonrpc_utils::{axum_utils::jsonrpc_router, stream::StreamServerConfig};
use tower::ServiceBuilder;

#[tokio::main]
async fn main() {
    let mut rpc = MetaIoHandler::with_compatibility(jsonrpc_core::Compatibility::V2);
    rpc.add_method("sleep", |params: Params| async move {
        let (x,): (u64,) = params.parse()?;
        tokio::time::sleep(Duration::from_secs(x)).await;
        Ok(x.into())
    });
    rpc.add_method("@ping", |_| async move { Ok("pong".into()) });
    rpc.add_method("value", |params: Params| async move {
        let x: Option<u64> = params.parse()?;
        Ok(x.unwrap_or_default().into())
    });
    rpc.add_method("add", |params: Params| async move {
        let ((x, y), z): ((i32, i32), i32) = params.parse()?;
        let sum = x + y + z;
        Ok(sum.into())
    });

    let rpc = Arc::new(rpc);
    let stream_config = StreamServerConfig::default()
        .with_channel_size(4)
        .with_pipeline_size(4);

    // HTTP and WS server.
    let ws_config = stream_config.clone().with_keep_alive(true);
    let app = jsonrpc_router("/rpc", rpc.clone(), ws_config).layer(
        ServiceBuilder::new()
            .layer(HandleErrorLayer::new(handle_timeout_error))
            .timeout(Duration::from_secs(4)),
    );

    // You can use additional tower-http middlewares to add e.g. CORS.
    tokio::spawn(async move {
        axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

async fn handle_timeout_error(err: BoxError) -> (StatusCode, String) {
    if err.is::<tower::timeout::error::Elapsed>() {
        (
            StatusCode::REQUEST_TIMEOUT,
            "Request took too long".to_string(),
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unhandled internal error: {}", err),
        )
    }
}
