use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::codec::{Framed, LinesCodec};

use crate::session::SessionRegistry;

const MAX_CONTROL_LINE_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ControlCommand {
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
}

impl ControlResponse {
    fn success(delivered: Option<usize>) -> Self {
        Self {
            success: true,
            error: None,
            delivered,
        }
    }

    fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            error: Some(error.into()),
            delivered: None,
        }
    }
}

pub async fn serve(listener: TcpListener, sessions: Arc<SessionRegistry>) -> std::io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let sessions = Arc::clone(&sessions);
                connections.spawn(async move {
                    if let Err(error) = handle_connection(stream, peer, sessions).await {
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
) -> std::io::Result<()> {
    let mut framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(MAX_CONTROL_LINE_BYTES),
    );
    while let Some(line) = framed.next().await {
        let line =
            line.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let response = match serde_json::from_str::<ControlCommand>(&line) {
            Ok(command) => process_command(command, &sessions),
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

fn process_command(command: ControlCommand, sessions: &SessionRegistry) -> ControlResponse {
    let action = match &command {
        ControlCommand::RegisterClient { .. } => "register_client",
        ControlCommand::PushUnicast { .. } => "push_unicast",
        ControlCommand::PushMulticast { .. } => "push_multicast",
        ControlCommand::PushBroadcast { .. } => "push_broadcast",
    };
    let result = match command {
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
