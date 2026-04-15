use anyhow::Context;
use chrono::Utc;
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

    println!("=== Price to Beat verification ===\n");
    println!("Connecting to Polymarket RTDS...");

    let (mut ws, _resp) =
        tokio_tungstenite::connect_async("wss://ws-live-data.polymarket.com")
            .await
            .context("connect failed")?;

    let sub_msg = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}"
        }]
    });
    ws.send(Message::Text(sub_msg.to_string().into())).await?;

    let (tx, mut rx) = tokio::sync::watch::channel(None::<(Decimal, i64)>);

    let feed_task = tokio::spawn(async move {
        let mut last_ping = tokio::time::Instant::now();
        loop {
            let msg = tokio::select! {
                m = ws.next() => match m {
                    Some(Ok(m)) => m,
                    _ => break,
                },
                _ = tokio::time::sleep(Duration::from_secs(30)) => break,
            };
            if last_ping.elapsed() >= Duration::from_secs(5) {
                let _ = ws.send(Message::text("PING")).await;
                last_ping = tokio::time::Instant::now();
            }
            let text = match &msg {
                Message::Text(t) => t.clone(),
                Message::Ping(d) => { let _ = ws.send(Message::Pong(d.clone())).await; continue; }
                _ => continue,
            };
            let val: serde_json::Value = match serde_json::from_str(text.as_ref()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if val.get("topic").and_then(|t| t.as_str()) != Some("crypto_prices_chainlink") {
                continue;
            }
            if let Some(payload) = val.get("payload") {
                if payload.get("symbol").and_then(|s| s.as_str()) == Some("btc/usd") {
                    if let Some(price_f64) = payload.get("value").and_then(|v| v.as_f64()) {
                        let ts = payload.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                        let price_str = format!("{:.2}", price_f64);
                        if let Ok(price) = Decimal::from_str(&price_str) {
                            tx.send_replace(Some((price, ts)));
                        }
                    }
                }
            }
        }
    });

    println!("Waiting for first price...");
    loop {
        rx.changed().await?;
        if rx.borrow().is_some() { break; }
    }
    let (first_price, _) = rx.borrow().unwrap();
    println!("Feed alive: BTC/USD = {}\n", first_price);

    let now_secs = Utc::now().timestamp();
    let current_window = now_secs - (now_secs % 300);
    let next_window = current_window + 300;
    let wait = (next_window - now_secs).max(0) as u64;
    let slug = format!("btc-updown-5m-{}", next_window);

    println!(
        "Next window: {} (in {}s)\nSlug: {}\nPolymarket URL: https://polymarket.com/event/{}\n",
        next_window, wait, slug, slug
    );
    println!("Open that URL in your browser NOW, then wait for the window to start.\n");

    if wait > 0 {
        println!("Sleeping {}s to window boundary...", wait);
        tokio::time::sleep(Duration::from_secs(wait)).await;
    }

    // At window boundary: capture price
    // Wait a tiny bit for the latest RTDS update to arrive
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = rx.changed().await;

    let (price_to_beat, feed_ts) = rx.borrow().unwrap();
    let capture_time = Utc::now();

    println!("========================================");
    println!("  WINDOW START: {}", next_window);
    println!("  CAPTURED AT:  {}", capture_time.format("%H:%M:%S%.3f UTC"));
    println!("  FEED TS:      {}ms", feed_ts);
    println!("  PRICE TO BEAT: ${}", price_to_beat);
    println!("========================================");
    println!("\nCompare this with the Polymarket page's 'Price To Beat' value.");
    println!("They should match within ~$1-2 (RTDS latency ~2s).\n");

    // Print prices for 15 more seconds so user can compare Current Price too
    println!("Streaming prices for 15 more seconds...\n");
    let end = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut count = 0u32;
    while tokio::time::Instant::now() < end {
        if rx.changed().await.is_ok() {
            if let Some((p, ts)) = *rx.borrow() {
                count += 1;
                println!(
                    "  [{:3}] ${:<12}  ts={}  local={}",
                    count, p, ts,
                    Utc::now().format("%H:%M:%S%.3f")
                );
            }
        }
    }

    println!("\nDone. Check Polymarket page for comparison.");
    feed_task.abort();
    Ok(())
}
