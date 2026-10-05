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
const IDLE_PING_AFTER: Duration = Duration::from_secs(30);
const PAGE_LIMIT: usize = 1000;
const MAX_PAGES: usize = 20;

pub struct WsSource {
    url: String,
    http_url: String,
    address: String,
    commitment: String,
    ws: Option<Ws>,
    http: reqwest::Client,
    full_tx: bool,
}

impl WsSource {
    pub fn new(
        url: impl Into<String>,
        address: impl Into<String>,
        commitment: impl Into<String>,
    ) -> Self {
        let url = url.into();
        let http_url = ws_to_http(&url);
        Self {
            url,
            http_url,
            address: address.into(),
            commitment: commitment.into(),
            ws: None,
            http: reqwest::Client::new(),
            full_tx: false,
        }
    }

    /// If true, backfill also calls `getTransaction` per signature to include
    /// `meta.logMessages` in the payload. Slower and rate-limit heavy; off by default.
    pub fn with_full_tx(mut self, full_tx: bool) -> Self {
        self.full_tx = full_tx;
        self
    }

    async fn connect(&mut self) -> Result<()> {
        let (mut ws, _) = connect_async(self.url.as_str())
            .await
            .map_err(|e| GuardError::Source(format!("connect failed: {e}")))?;
        let req = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [ { "mentions": [self.address] }, { "commitment": self.commitment } ]
        });
        ws.send(Message::Text(req.to_string()))
            .await
            .map_err(|e| GuardError::Source(format!("subscribe failed: {e}")))?;
        self.ws = Some(ws);
        Ok(())
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params});
        let resp = self
            .http
            .post(&self.http_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| GuardError::Source(format!("http error: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(GuardError::Source(format!("http {status}")));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| GuardError::Source(format!("json: {e}")))?;
        if let Some(err) = v.get("error") {
            return Err(GuardError::Source(format!("rpc error: {err}")));
        }
        Ok(v.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn fetch_tx_payload(&self, sig: &str) -> Result<Value> {
        let params = json!([sig, {"encoding":"json","maxSupportedTransactionVersion":0}]);
        self.rpc("getTransaction", params).await
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
                Err(_) => {
                    if ws.send(Message::Ping(Vec::new())).await.is_err() {
                        self.ws = None;
                        return Ok(None);
                    }
                }
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

    /// Fetch events strictly newer than `after_signature`, oldest first.
    /// Uses `before`-pagination on `getSignaturesForAddress` (avoids the known
    /// `until` inconsistency). Dedupe at the store makes any overlap harmless.
    async fn backfill(&mut self, after_signature: Option<&str>) -> Result<Vec<Event>> {
        let after = match after_signature {
            Some(s) if !s.is_empty() => s,
            _ => return Ok(Vec::new()),
        };

        let mut collected: Vec<Value> = Vec::new();
        let mut before: Option<String> = None;
        let mut found = false;

        for _ in 0..MAX_PAGES {
            let mut params = json!([self.address, {"limit": PAGE_LIMIT}]);
            if let Some(b) = &before {
                params[1]["before"] = json!(b);
            }
            let page = self.rpc("getSignaturesForAddress", params).await?;
            let items = page.as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                break;
            }
            let (newer, hit) = split_at_after(&items, after);
            collected.extend(newer);
            if hit {
                found = true;
                break;
            }
            if items.len() < PAGE_LIMIT {
                break;
            }
            before = items
                .last()
                .and_then(|v| v.get("signature"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            if before.is_none() {
                break;
            }
        }

        if !found {
            tracing::warn!(
                "backfill: did not reach checkpoint signature within {} pages; \
                 using best effort ({} events)",
                MAX_PAGES,
                collected.len()
            );
        }

        // collected is newest-first -> reverse to oldest-first.
        collected.reverse();

        let mut events = Vec::with_capacity(collected.len());
        for item in collected {
            let signature = item
                .get("signature")
                .and_then(|s| s.as_str())
                .ok_or_else(|| GuardError::Source("missing signature in sig page".into()))?
                .to_string();
            let slot = item.get("slot").and_then(|s| s.as_u64()).unwrap_or(0);
            let payload = if self.full_tx {
                match self.fetch_tx_payload(&signature).await {
                    Ok(tx) => tx,
                    Err(e) => {
                        tracing::warn!("backfill: getTransaction({signature}) failed: {e}");
                        item
                    }
                }
            } else {
                item
            };
            events.push(Event { signature, ix_index: 0, slot, payload });
        }
        Ok(events)
    }
}

/// Convert a websocket URL into the matching HTTP JSON-RPC URL.
fn ws_to_http(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        url.to_string()
    }
}

/// Given a newest-first page and the checkpoint signature, return
/// (items strictly newer than `after`, whether `after` was found).
fn split_at_after(page: &[Value], after: &str) -> (Vec<Value>, bool) {
    let mut out = Vec::with_capacity(page.len());
    for item in page {
        if item.get("signature").and_then(|s| s.as_str()) == Some(after) {
            return (out, true);
        }
        out.push(item.clone());
    }
    (out, false)
}

fn parse_notification(text: &str) -> Option<Event> {
    let v: Value = serde_json::from_str(text).ok()?;
    if v.get("method")?.as_str()? != "logsNotification" {
        return None;
    }
    let result = v.get("params")?.get("result")?;
    let slot = result.get("context")?.get("slot")?.as_u64()?;
    let value = result.get("value")?;
    let signature = value.get("signature")?.as_str()?.to_string();
    Some(Event { signature, ix_index: 0, slot, payload: value.clone() })
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
        assert!(parse_notification(r#"{"jsonrpc":"2.0","result":24040,"id":1}"#).is_none());
    }

    #[test]
    fn ignores_garbage() {
        assert!(parse_notification("not json").is_none());
    }

    #[test]
    fn ws_to_http_converts_schemes() {
        assert_eq!(
            ws_to_http("wss://api.devnet.solana.com"),
            "https://api.devnet.solana.com"
        );
        assert_eq!(ws_to_http("ws://localhost:8900"), "http://localhost:8900");
        assert_eq!(ws_to_http("https://already-http"), "https://already-http");
    }

    #[test]
    fn split_stops_at_after_signature() {
        let page = vec![
            json!({"signature":"d","slot":4}),
            json!({"signature":"c","slot":3}),
            json!({"signature":"b","slot":2}),
            json!({"signature":"a","slot":1}),
        ];
        let (newer, found) = split_at_after(&page, "b");
        assert!(found);
        assert_eq!(newer.len(), 2);
        assert_eq!(newer[0]["signature"], "d");
        assert_eq!(newer[1]["signature"], "c");
    }

    #[test]
    fn split_returns_all_when_after_not_present() {
        let page = vec![json!({"signature":"x","slot":9})];
        let (newer, found) = split_at_after(&page, "nope");
        assert!(!found);
        assert_eq!(newer.len(), 1);
    }

    #[test]
    fn split_when_after_is_the_head_gives_empty() {
        let page = vec![
            json!({"signature":"head","slot":9}),
            json!({"signature":"tail","slot":8}),
        ];
        let (newer, found) = split_at_after(&page, "head");
        assert!(found);
        assert!(newer.is_empty());
    }
}
