use std::time::Duration;

use solana_event_guard::{Source, WsSource};

#[tokio::main]
async fn main() {
    let url = std::env::var("SOLANA_WS_URL")
        .unwrap_or_else(|_| "wss://api.devnet.solana.com".into());
    // The System Program is mentioned by almost every transaction: very busy, good for a quick test.
    let address = std::env::var("WATCH_ADDRESS")
        .unwrap_or_else(|_| "11111111111111111111111111111111".into());

    println!("connecting to {url}, watching {address}");
    let mut src = WsSource::new(url, address, "confirmed");

    let mut seen = 0;
    while seen < 10 {
        match src.next().await {
            Ok(Some(ev)) => {
                seen += 1;
                println!("#{seen} slot={} sig={}", ev.slot, ev.signature);
            }
            Ok(None) => println!("connection dropped, reconnecting..."),
            Err(e) => {
                eprintln!("error: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    println!("done: received {seen} events");
}
