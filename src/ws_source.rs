use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::error::{GuardError, Result};
use crate::event::Event;
use crate::source::Source;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// If nothing arrives for this long, send a ping so idle connections are not dropped.
const IDLE_PING_AFTER: Duration = Duration::from_secs(30);

/// Live events from a Solana RPC WebSocket via `logsSubscribe` (mentions one address).
pub struct WsSource {
    url: String,
    address: String,
    commitment: String,
    ws: Option<Ws>,
}

impl WsSource {
    pub fn new(
        url: impl Into<String>,
        address: impl Into<String>,
        commitment: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            address: address.into(),
            commitment: commitment.into(),
            ws: None,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        let (mut ws, _) = connect_async(self.url.as_str())
            .await
            .map_err(|e| GuardError::Source(format!("connect failed: {e}")))?;

        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                { "mentions": [self.address] },
                { "commitment": self.commitment }
            ]
        });
        ws.send(Message::Text(req.to_string()))
            .await
            .map_err(|e| GuardError::Source(format!("subscribe failed: {e}")))?;

        self.ws = Some(ws);
        Ok(())
    }
}

#[async_trait]
impl Source for WsSource {
    async fn next(&mut self) -> Result<Option<Event>> {
        if self.ws.is_none() {
            self.connect().await?;
        }
        loop {
            let ws = self.ws.as_mut().expect("connected above");
            match tokio::time::timeout(IDLE_PING_AFTER, ws.next()).await {
                // Idle: ping to keep the connection alive.
                Err(_) => {
                    if ws.send(Message::Ping(Vec::new())).await.is_err() {
                        self.ws = None;
                        return Ok(None);
                    }
                }
                // Stream ended.
                Ok(None) => {
                    self.ws = None;
                    return Ok(None);
                }
                Ok(Some(Err(e))) => {
                    tracing::warn!("websocket error: {e}");
                    self.ws = None;
                    return Ok(None);
                }
                Ok(Some(Ok(Message::Text(text)))) => {
                    if let Some(event) = parse_notification(&text) {
                        return Ok(Some(event));
                    }
                }
                Ok(Some(Ok(Message::Close(_)))) => {
                    self.ws = None;
                    return Ok(None);
                }
                Ok(Some(Ok(_))) => {}
            }
        }
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.ws = None;
        self.connect().await
    }

    async fn backfill(&mut self, _after_signature: Option<&str>) -> Result<Vec<Event>> {
        // Week 2: getSignaturesForAddress with an overlap window.
        Err(GuardError::Source("backfill not implemented yet (week 2)".into()))
    }
}

/// Turn a `logsNotification` message into an `Event`. Other messages (e.g. the
/// subscription confirmation) return `None`.
fn parse_notification(text: &str) -> Option<Event> {
    let v: Value = serde_json::from_str(text).ok()?;
    if v.get("method")?.as_str()? != "logsNotification" {
        return None;
    }
    let result = v.get("params")?.get("result")?;
    let slot = result.get("context")?.get("slot")?.as_u64()?;
    let value = result.get("value")?;
    let signature = value.get("signature")?.as_str()?.to_string();
    Some(Event {
        signature,
        ix_index: 0,
        slot,
        payload: value.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_logs_notification() {
        let msg = r#"{"jsonrpc":"2.0","method":"logsNotification","params":{"result":{"context":{"slot":5208469},"value":{"signature":"sig123","err":null,"logs":["log one"]}},"subscription":24040}}"#;
        let e = parse_notification(msg).expect("should parse");
        assert_eq!(e.signature, "sig123");
        assert_eq!(e.slot, 5208469);
        assert_eq!(e.ix_index, 0);
    }

    #[test]
    fn ignores_subscription_confirmation() {
        let msg = r#"{"jsonrpc":"2.0","result":24040,"id":1}"#;
        assert!(parse_notification(msg).is_none());
    }

    #[test]
    fn ignores_garbage() {
        assert!(parse_notification("not json").is_none());
    }
}
