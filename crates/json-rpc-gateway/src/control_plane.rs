use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::codec::{Framed, LinesCodec};

use crate::components::{ComponentInventory, Components};
use crate::session::SessionRegistry;

const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ControlCommand {
    ListComponents,
    ListMethods,
    ListTransports,
    SetTransportEnabled {
        transport: String,
        enabled: bool,
    },
    RegisterClient {
        connection_id: String,
        client_id: String,
        #[serde(default)]
        groups: Vec<String>,
    },
    PushUnicast {
        client_id: String,
        payload: String,
    },
    PushMulticast {
        group_id: String,
        payload: String,
    },
    PushBroadcast {
        payload: String,
    },
}

#[derive(Serialize)]
struct ControlResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivered: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    components: Option<Components>,
    #[serde(skip_serializing_if = "Option::is_none")]
    methods: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    method_details: Option<Vec<json_rpc_2_server::MethodDescription>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    complete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    errors: Option<Vec<json_rpc_balancer::MethodDiscoveryError>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transports: Option<Vec<crate::transport_policy::TransportStatus>>,
}

impl ControlResponse {
    fn success(delivered: Option<usize>) -> Self {
        Self {
            success: true,
            error: None,
            delivered,
            components: None,
            methods: None,
            method_details: None,
            complete: None,
            errors: None,
            transports: None,
        }
    }

    fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            error: Some(error.into()),
            delivered: None,
            components: None,
            methods: None,
            method_details: None,
            complete: None,
            errors: None,
            transports: None,
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    sessions: Arc<SessionRegistry>,
    inventory: Arc<ComponentInventory>,
) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let sessions = Arc::clone(&sessions);
                let inventory = Arc::clone(&inventory);
                connections.spawn(async move {
                    if let Err(error) = handle_connection(stream, peer, sessions, inventory).await {
                        tracing::warn!(%peer, %error, "control-plane connection ended with an error");
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    tracing::error!(%error, "control-plane connection docs failed");
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    sessions: Arc<SessionRegistry>,
    inventory: Arc<ComponentInventory>,
) -> std::io::Result<()> {
    let mut framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(MAX_CONTROL_LINE_BYTES),
    );
    while let Some(line) = framed.next().await {
        let line =
            line.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let response = match serde_json::from_str::<ControlCommand>(&line) {
            Ok(command) => process_command(command, &sessions, &inventory).await,
            Err(error) => {
                tracing::warn!(%peer, %error, "invalid control-plane command");
                ControlResponse::failure("InvalidCommand")
            }
        };
        let response = serde_json::to_string(&response).map_err(std::io::Error::other)?;
        tracing::debug!(
            %peer,
            request_direction = "control_plane_peer_to_gateway",
            response_direction = "gateway_to_control_plane_peer",
            request = %line,
            response = %response,
            "control-plane exchange completed"
        );
        framed
            .send(response)
            .await
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))?;
    }
    Ok(())
}

async fn process_command(
    command: ControlCommand,
    sessions: &SessionRegistry,
    inventory: &ComponentInventory,
) -> ControlResponse {
    if matches!(
        command,
        ControlCommand::ListTransports | ControlCommand::SetTransportEnabled { .. }
    ) {
        let Some(policy) = &inventory.transport_policy else {
            return ControlResponse::failure("TransportPolicyUnavailable");
        };
        if let ControlCommand::SetTransportEnabled { transport, enabled } = command
            && let Err(error) = policy.set_enabled(&transport, enabled).await
        {
            tracing::warn!(%transport, enabled, %error, "transport policy command rejected");
            return ControlResponse::failure(error);
        }
        let mut response = ControlResponse::success(None);
        response.transports = Some(policy.snapshot());
        return response;
    }
    if matches!(command, ControlCommand::ListMethods) {
        let result = inventory.list_methods().await;
        let mut response = if result.complete {
            ControlResponse::success(None)
        } else {
            ControlResponse::failure("MethodDiscoveryIncomplete")
        };
        response.methods = Some(result.methods);
        response.method_details = Some(result.method_details);
        response.complete = Some(result.complete);
        response.errors = Some(result.errors);
        return response;
    }
    if matches!(command, ControlCommand::ListComponents) {
        tracing::debug!(action = "list_components", "control-plane command accepted");
        let mut response = ControlResponse::success(None);
        response.components = Some(inventory.snapshot());
        return response;
    }
    process_session_command(command, sessions)
}

fn process_session_command(command: ControlCommand, sessions: &SessionRegistry) -> ControlResponse {
    let action = match &command {
        ControlCommand::ListComponents
        | ControlCommand::ListMethods
        | ControlCommand::ListTransports
        | ControlCommand::SetTransportEnabled { .. } => unreachable!(),
        ControlCommand::RegisterClient { .. } => "register_client",
        ControlCommand::PushUnicast { .. } => "push_unicast",
        ControlCommand::PushMulticast { .. } => "push_multicast",
        ControlCommand::PushBroadcast { .. } => "push_broadcast",
    };
    let result = match command {
        ControlCommand::ListComponents
        | ControlCommand::ListMethods
        | ControlCommand::ListTransports
        | ControlCommand::SetTransportEnabled { .. } => unreachable!(),
        ControlCommand::RegisterClient {
            connection_id,
            client_id,
            groups,
        } => sessions
            .register_client(&connection_id, client_id, groups)
            .map(|()| None),
        ControlCommand::PushUnicast { client_id, payload } => sessions
            .push_unicast(&client_id, Bytes::from(payload))
            .map(|()| None),
        ControlCommand::PushMulticast { group_id, payload } => sessions
            .push_multicast(&group_id, Bytes::from(payload))
            .map(Some),
        ControlCommand::PushBroadcast { payload } => {
            sessions.push_broadcast(Bytes::from(payload)).map(Some)
        }
    };

    match result {
        Ok(delivered) => {
            tracing::debug!(action, ?delivered, "control-plane command accepted");
            ControlResponse::success(delivered)
        }
        Err(error) => {
            tracing::warn!(action, %error, "control-plane command rejected");
            ControlResponse::failure(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::ListenerInfo;
    use json_rpc_balancer::{BackendConfig, BackendPool};
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn list_methods_unions_live_registries_and_reports_partial_failure() {
        timeout(Duration::from_secs(15), async {
            let mut servers = Vec::new();
            let mut configs = Vec::new();
            for (id, method) in [("backend-a", "alpha"), ("backend-b", "beta")] {
                let rpc = json_rpc_2_server::RpcServer::new();
                rpc.register(method, |params| async { Ok(params) }).unwrap();
                rpc.register("shared", |params| async { Ok(params) })
                    .unwrap();
                rpc.document_method(method, serde_json::json!({
                    "type":"object","required":["message"],"properties":{"message":{"type":"string"}}
                }), serde_json::json!({"message":"mock"})).unwrap();
                rpc.register_alias(format!("rpc.{method}"), method).unwrap();
                let mut server = json_rpc_2_runtime::MultiTransportServer::new(Arc::new(
                    gateway_protocol::GatewayBackendProtocol::new(Arc::new(rpc)),
                ));
                server
                    .set_bind(["127.0.0.1".parse().unwrap()])
                    .set_tcp_port(0);
                let running = server.start().await.unwrap();
                configs.push(BackendConfig {
                    id: id.into(),
                    host: "127.0.0.1".into(),
                    ports: [("tcp".into(), running.listeners().next().unwrap().1.port())]
                        .into_iter()
                        .collect(),
                    pool_size: 1,
                    options: json_rpc_balancer::BackendOptions {
                        operation_timeout_ms: 200,
                        ..Default::default()
                    },
                });
                servers.push(running);
            }
            let pool = Arc::new(BackendPool::new(configs).unwrap());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let inventory = Arc::new(ComponentInventory::new(
                std::time::Instant::now(),
                vec![],
                address,
                "127.0.0.1:9090".parse().unwrap(),
                pool,
            ));
            let task = tokio::spawn(serve(listener, SessionRegistry::new(), inventory));
            let mut framed = Framed::new(
                TcpStream::connect(address).await.unwrap(),
                LinesCodec::new(),
            );
            framed
                .send(r#"{"action":"list_methods"}"#.to_owned())
                .await
                .unwrap();
            let response: serde_json::Value =
                serde_json::from_str(&framed.next().await.unwrap().unwrap()).unwrap();
            let mut base_response = response.clone();
            base_response.as_object_mut().unwrap().remove("method_details");
            assert_eq!(
                base_response,
                serde_json::json!({
                    "success":true, "complete":true, "errors":[],
                    "methods":["alpha","beta","rpc.alpha","rpc.beta","rpc.list_methods","shared"]
                })
            );
            let details = response["method_details"].as_array().unwrap();
            assert_eq!(details.len(), 6);
            let alpha = details.iter().find(|detail| detail["name"] == "alpha").unwrap();
            assert_eq!(alpha["params_schema"]["required"], serde_json::json!(["message"]));
            assert_eq!(alpha["example_request"]["params"], serde_json::json!({"message":"mock"}));
            let alias = details.iter().find(|detail| detail["name"] == "rpc.alpha").unwrap();
            assert_eq!(alias["params_schema"], alpha["params_schema"]);
            assert_eq!(alias["example_request"]["method"], "rpc.alpha");
            let unknown = details.iter().find(|detail| detail["name"] == "shared").unwrap();
            assert!(unknown["params_schema"].is_null());
            servers.pop().unwrap().close().await.unwrap();
            framed
                .send(r#"{"action":"list_methods"}"#.to_owned())
                .await
                .unwrap();
            let response: serde_json::Value =
                serde_json::from_str(&framed.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(response["success"], false);
            assert_eq!(response["complete"], false);
            assert_eq!(response["error"], "MethodDiscoveryIncomplete");
            assert_eq!(response["errors"][0]["backend_id"], "backend-b");
            assert_eq!(response["method_details"].as_array().unwrap().len(), 4);
            assert_eq!(
                response["methods"],
                serde_json::json!(["alpha", "rpc.alpha", "rpc.list_methods", "shared"])
            );
            servers.pop().unwrap().close().await.unwrap();
            framed
                .send(r#"{"action":"list_methods"}"#.to_owned())
                .await
                .unwrap();
            let response: serde_json::Value =
                serde_json::from_str(&framed.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(response["methods"], serde_json::json!([]));
            assert_eq!(response["errors"].as_array().unwrap().len(), 2);
            assert_eq!(response["complete"], false);
            task.abort();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn list_components_reports_local_state_without_secrets_or_health_claims() {
        timeout(Duration::from_secs(5), async {
            let options = json_rpc_balancer::BackendOptions {
                mqtt_url_env: Some("PRIVATE_BROKER_SECRET".into()),
                ..Default::default()
            };
            let pool = Arc::new(
                BackendPool::new(vec![BackendConfig {
                    id: "backend-a".into(),
                    host: "192.0.2.1".into(),
                    ports: [("tcp".into(), 8081)].into_iter().collect(),
                    pool_size: 2,
                    options,
                }])
                .unwrap(),
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let inventory = Arc::new(ComponentInventory::new(
                std::time::Instant::now() - Duration::from_secs(2),
                vec![ListenerInfo {
                    transport: "tcp".into(),
                    address: "127.0.0.1:8443".parse().unwrap(),
                }],
                address,
                "127.0.0.1:9090".parse().unwrap(),
                pool,
            ));
            let task = tokio::spawn(serve(listener, SessionRegistry::new(), inventory));
            let stream = TcpStream::connect(address).await.unwrap();
            let mut framed = Framed::new(stream, LinesCodec::new());
            framed
                .send(r#"{"action":"list_components"}"#.to_owned())
                .await
                .unwrap();
            let raw = framed.next().await.unwrap().unwrap();
            assert!(!raw.contains("PRIVATE_BROKER_SECRET"));
            let response: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(response["success"], true);
            let components = &response["components"];
            assert_eq!(components["gateway"]["pid"], std::process::id());
            assert_eq!(components["gateway"]["version"], env!("CARGO_PKG_VERSION"));
            assert!(components["gateway"]["uptime_seconds"].as_u64().unwrap() >= 2);
            assert_eq!(components["gateway"]["listeners"][0]["transport"], "tcp");
            assert_eq!(
                components["gateway"]["control_address"],
                address.to_string()
            );
            assert_eq!(components["balancer"]["execution"], "in_process");
            assert_eq!(components["balancer"]["backend_count"], 1);
            let backend = &components["backends"][0];
            assert_eq!(backend["id"], "backend-a");
            assert_eq!(backend["host"], "192.0.2.1");
            let transport = &backend["transports"][0];
            assert_eq!(transport["pool_size"], 2);
            assert_eq!(transport["empty_idle_slots"], 2);
            assert_eq!(transport["connected_idle_slots"], 0);
            assert_eq!(transport["busy_slots"], 0);
            assert_eq!(transport["circuit"]["state"], "closed");
            assert!(backend.get("pid").is_none());
            framed
                .send(r#"{"action":"invalid"}"#.to_owned())
                .await
                .unwrap();
            let failure: serde_json::Value =
                serde_json::from_str(&framed.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(
                failure,
                serde_json::json!({"success":false,"error":"InvalidCommand"})
            );
            task.abort();
        })
        .await
        .unwrap();
    }
}
