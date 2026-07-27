//! Async websocket client for the state-history plugin.
//!
//! Protocol: on connect the server pushes its ABI as a single text frame;
//! everything after that is binary, ABI-encoded `request` / `result`
//! variants. Flow control is credit-based — the server sends at most
//! `max_messages_in_flight` block results until acked.

use crate::types::{
    encode_get_blocks_ack_request, encode_get_blocks_request, encode_get_status_request,
    GetBlocksRequest, GetStatusResult, Result, ShipError, ShipResult,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

#[derive(Debug, Clone)]
pub struct ShipConfig {
    /// e.g. `ws://127.0.0.1:8080`
    pub url: String,
    pub max_messages_in_flight: u32,
}

impl Default for ShipConfig {
    fn default() -> Self {
        ShipConfig {
            url: "ws://127.0.0.1:8080".to_string(),
            max_messages_in_flight: 128,
        }
    }
}

pub struct ShipClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// The state-history ABI as announced by the server (JSON).
    pub abi: serde_json::Value,
}

impl ShipClient {
    /// Connect and complete the handshake (receive the server's ABI).
    pub async fn connect(url: &str) -> Result<Self> {
        let (ws, _response) = connect_async(url).await?;
        let mut client = ShipClient {
            ws,
            abi: serde_json::Value::Null,
        };
        loop {
            match client.recv_raw().await? {
                Message::Text(text) => {
                    client.abi = serde_json::from_str(&text)
                        .map_err(|e| ShipError::Protocol(format!("invalid ABI frame: {e}")))?;
                    return Ok(client);
                }
                Message::Binary(_) => {
                    return Err(ShipError::Protocol(
                        "expected ABI text frame before binary traffic".into(),
                    ))
                }
                _ => continue,
            }
        }
    }

    async fn recv_raw(&mut self) -> Result<Message> {
        loop {
            match self.ws.next().await {
                Some(Ok(msg @ (Message::Text(_) | Message::Binary(_)))) => return Ok(msg),
                Some(Ok(Message::Close(_))) | None => return Err(ShipError::Closed),
                Some(Ok(_)) => continue, // ping/pong handled by tungstenite
                Some(Err(e)) => return Err(e.into()),
            }
        }
    }

    async fn send(&mut self, data: Vec<u8>) -> Result<()> {
        self.ws.send(Message::Binary(data.into())).await?;
        Ok(())
    }

    /// Fetch the chain/state-history status.
    pub async fn get_status(&mut self) -> Result<GetStatusResult> {
        self.send(encode_get_status_request()).await?;
        loop {
            match self.next_result().await? {
                ShipResult::Status(status) => return Ok(status),
                // Block results may still be in flight from an earlier
                // request; let the caller's normal loop consume them.
                ShipResult::Blocks(_) => continue,
            }
        }
    }

    /// Start (or restart) the block stream.
    pub async fn request_blocks(&mut self, req: &GetBlocksRequest) -> Result<()> {
        self.send(encode_get_blocks_request(req)).await
    }

    /// Grant the server credit for `num_messages` more results.
    pub async fn ack_blocks(&mut self, num_messages: u32) -> Result<()> {
        self.send(encode_get_blocks_ack_request(num_messages)).await
    }

    /// Receive and decode the next result frame.
    pub async fn next_result(&mut self) -> Result<ShipResult> {
        loop {
            match self.recv_raw().await? {
                Message::Binary(data) => return ShipResult::decode(&data),
                // A duplicate ABI frame would indicate a proxy restart; skip.
                Message::Text(_) => continue,
                _ => unreachable!("recv_raw only yields text/binary"),
            }
        }
    }
}
