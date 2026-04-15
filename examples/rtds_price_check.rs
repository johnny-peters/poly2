use anyhow::Context;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::ring::default_provider(),
    );

    println!("Connecting to Polymarket RTDS WebSocket (persistent)...");

    let (mut ws, _resp) =
        tokio_tungstenite::connect_async("wss://ws-live-data.polymarket.com")
            .await
            .context("RTDS WebSocket connect failed")?;

    println!("Connected. Subscribing to crypto_prices_chainlink (btc/usd)...");

    let sub_msg = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}"
        }]
    });
    ws.send(Message::Text(sub_msg.to_string().into()))
        .await
        .context("subscribe send failed")?;

    println!("Subscribed. Streaming real-time BTC/USD (Ctrl+C to stop)...\n");

    let mut last_ping = tokio::time::Instant::now();
    let mut count = 0u32;

    loop {
        let msg = tokio::select! {
            m = ws.next() => match m {
                Some(Ok(m)) => m,
                Some(Err(e)) => { eprintln!("ws error: {e}"); break; }
                None => { eprintln!("ws closed"); break; }
            },
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                println!("No message for 30s");
                break;
            }
        };

        if last_ping.elapsed() >= Duration::from_secs(5) {
            let _ = ws.send(Message::text("PING")).await;
            last_ping = tokio::time::Instant::now();
        }

        let text = match &msg {
            Message::Text(t) => t.clone(),
            Message::Ping(d) => {
                let _ = ws.send(Message::Pong(d.clone())).await;
                continue;
            }
            _ => continue,
        };

        let val: serde_json::Value = match serde_json::from_str(text.as_ref()) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let topic = val.get("topic").and_then(|t| t.as_str()).unwrap_or("");
        if topic != "crypto_prices_chainlink" {
            continue;
        }

        if let Some(payload) = val.get("payload") {
            if payload.get("symbol").and_then(|s| s.as_str()) == Some("btc/usd") {
                let value = payload.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let ts = payload.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                let now_ms = chrono::Utc::now().timestamp_millis();
                let price_str = format!("{:.2}", value);
                let price = Decimal::from_str(&price_str).unwrap_or_default();
                count += 1;
                println!(
                    "[{:4}] ${:<12}  feed_ts={}  latency={}ms  local={}",
                    count,
                    price,
                    ts,
                    now_ms - ts,
                    chrono::Utc::now().format("%H:%M:%S%.3f"),
                );
                if count >= 30 {
                    break;
                }
            }
        }
    }

    let _ = ws.close(None).await;
    println!("\nDone.");
    Ok(())
}
