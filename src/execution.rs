use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use hmac::{Hmac, KeyInit, Mac as _};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashSet;
use std::env;
use std::str::FromStr;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use thiserror::Error;

use crate::types::{Side, StrategySignal};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionStatus {
    Pending,
    PartiallyFilled,
    Filled,
    Rejected,
}

#[derive(Clone, Debug)]
pub struct ExecutionFill {
    pub order_id: Option<String>,
    pub fill_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub fee: Decimal,
    /// True if this fill was from a sell (close) order.
    pub sell: bool,
}

#[derive(Clone, Debug)]
pub struct ExecutionReport {
    pub market_id: String,
    pub action_count: usize,
    pub order_ids: Vec<String>,
    pub status: ExecutionStatus,
    pub fills: Vec<ExecutionFill>,
    pub signal: StrategySignal,
}

#[derive(Clone, Debug)]
pub struct ExecutionEvent {
    pub event_id: u64,
    pub occurred_at: DateTime<Utc>,
    pub event_type: ExecutionEventType,
    pub market_id: String,
    pub order_id: Option<String>,
    pub status: ExecutionStatus,
    pub fill: Option<ExecutionFill>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionEventType {
    FillDelta,
    ReportSummary,
    Error,
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("execution backend failure: {0}")]
    Backend(String),
}

#[async_trait]
pub trait ExecutionClient: Send + Sync {
    async fn submit(&self, signal: &StrategySignal) -> Result<ExecutionReport, ExecutionError>;
}

pub struct EventingExecutionClient<C> {
    inner: C,
    tx: broadcast::Sender<ExecutionEvent>,
    event_counter: AtomicU64,
}

impl<C> EventingExecutionClient<C> {
    pub fn new(inner: C, buffer: usize) -> Self {
        let (tx, _) = broadcast::channel(buffer);
        Self {
            inner,
            tx,
            event_counter: AtomicU64::new(1),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.tx.subscribe()
    }

    fn next_event_id(&self) -> u64 {
        self.event_counter.fetch_add(1, Ordering::Relaxed)
    }
}

pub struct RetryingExecutionClient<C> {
    inner: C,
    max_retries: u32,
    retry_backoff_ms: u64,
}

impl<C> RetryingExecutionClient<C> {
    pub fn new(inner: C, max_retries: u32, retry_backoff_ms: u64) -> Self {
        Self {
            inner,
            max_retries,
            retry_backoff_ms,
        }
    }
}

#[derive(Clone, Default)]
pub struct MockExecutionClient {
    submitted: Arc<Mutex<Vec<StrategySignal>>>,
}

impl MockExecutionClient {
    pub fn submitted(&self) -> Vec<StrategySignal> {
        self.submitted
            .lock()
            .expect("mock execution mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl ExecutionClient for MockExecutionClient {
    async fn submit(&self, signal: &StrategySignal) -> Result<ExecutionReport, ExecutionError> {
        self.submitted
            .lock()
            .map_err(|_| ExecutionError::Backend("mutex poisoned".to_string()))?
            .push(signal.clone());

        Ok(ExecutionReport {
            market_id: signal.market_id.clone(),
            action_count: signal.actions.len(),
            order_ids: Vec::new(),
            status: ExecutionStatus::Filled,
            fills: signal
                .actions
                .iter()
                .map(|a| ExecutionFill {
                    order_id: None,
                    fill_id: None,
                    side: a.side.clone(),
                    price: a.price,
                    size: a.size,
                    fee: Decimal::ZERO,
                    sell: a.sell,
                })
                .collect(),
            signal: signal.clone(),
        })
    }
}

#[async_trait]
impl<C> ExecutionClient for RetryingExecutionClient<C>
where
    C: ExecutionClient,
{
    async fn submit(&self, signal: &StrategySignal) -> Result<ExecutionReport, ExecutionError> {
        let mut attempts = 0_u32;
        loop {
            match self.inner.submit(signal).await {
                Ok(report) => return Ok(report),
                Err(err) if attempts < self.max_retries => {
                    attempts += 1;
                    sleep(Duration::from_millis(self.retry_backoff_ms)).await;
                    let _ = err;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

#[async_trait]
impl<C> ExecutionClient for EventingExecutionClient<C>
where
    C: ExecutionClient,
{
    async fn submit(&self, signal: &StrategySignal) -> Result<ExecutionReport, ExecutionError> {
        let report = match self.inner.submit(signal).await {
            Ok(report) => report,
            Err(err) => {
                let _ = self.tx.send(ExecutionEvent {
                    event_id: self.next_event_id(),
                    occurred_at: Utc::now(),
                    event_type: ExecutionEventType::Error,
                    market_id: signal.market_id.clone(),
                    order_id: None,
                    status: ExecutionStatus::Rejected,
                    fill: None,
                    error: Some(err.to_string()),
                });
                return Err(err);
            }
        };
        for fill in &report.fills {
            let _ = self.tx.send(ExecutionEvent {
                event_id: self.next_event_id(),
                occurred_at: Utc::now(),
                event_type: ExecutionEventType::FillDelta,
                market_id: report.market_id.clone(),
                order_id: fill.order_id.clone(),
                status: report.status.clone(),
                fill: Some(fill.clone()),
                error: None,
            });
        }
        let _ = self.tx.send(ExecutionEvent {
            event_id: self.next_event_id(),
            occurred_at: Utc::now(),
            event_type: ExecutionEventType::ReportSummary,
            market_id: report.market_id.clone(),
            order_id: report.order_ids.first().cloned(),
            status: report.status.clone(),
            fill: None,
            error: None,
        });
        Ok(report)
    }
}

#[derive(Clone)]
pub struct FlakyExecutionClient {
    fail_times: Arc<Mutex<u32>>,
    attempts: Arc<Mutex<u32>>,
}

impl FlakyExecutionClient {
    pub fn new(fail_times: u32) -> Self {
        Self {
            fail_times: Arc::new(Mutex::new(fail_times)),
            attempts: Arc::new(Mutex::new(0)),
        }
    }

    pub fn attempts(&self) -> u32 {
        *self.attempts.lock().expect("flaky attempts mutex poisoned")
    }
}

#[async_trait]
impl ExecutionClient for FlakyExecutionClient {
    async fn submit(&self, signal: &StrategySignal) -> Result<ExecutionReport, ExecutionError> {
        {
            let mut attempts = self
                .attempts
                .lock()
                .map_err(|_| ExecutionError::Backend("mutex poisoned".to_string()))?;
            *attempts += 1;
        }
        let mut remaining_failures = self
            .fail_times
            .lock()
            .map_err(|_| ExecutionError::Backend("mutex poisoned".to_string()))?;
        if *remaining_failures > 0 {
            *remaining_failures -= 1;
            return Err(ExecutionError::Backend("simulated failure".to_string()));
        }

        Ok(ExecutionReport {
            market_id: signal.market_id.clone(),
            action_count: signal.actions.len(),
            order_ids: Vec::new(),
            status: ExecutionStatus::Filled,
            fills: signal
                .actions
                .iter()
                .map(|a| ExecutionFill {
                    order_id: None,
                    fill_id: None,
                    side: a.side.clone(),
                    price: a.price,
                    size: a.size,
                    fee: Decimal::ZERO,
                    sell: a.sell,
                })
                .collect(),
            signal: signal.clone(),
        })
    }
}

pub struct PolymarketHttpExecutionClient {
    http: reqwest::Client,
    api_base_url: String,
    api_key: Option<String>,
    funder: Option<String>,
    signature_type: Option<u8>,
    per_order_max_retries: u32,
    per_order_backoff_ms: u64,
    status_poll_attempts: u32,
    status_poll_interval_ms: u64,
    nonce_counter: AtomicU64,
    signature_provider: Option<Arc<SignatureProvider>>,
}

type SignatureProvider =
    dyn Fn(&OrderSignContext) -> Option<String> + Send + Sync;

#[derive(Clone, Debug)]
pub struct OrderSignContext {
    pub market_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub strategy_id: String,
    pub timestamp: String,
    pub nonce: String,
    pub method: String,
    pub path: String,
    pub body: String,
}

#[derive(Clone, Debug, Serialize)]
struct PolymarketOrderRequest {
    market_id: String,
    side: String,
    price: String,
    size: String,
    strategy_id: String,
    /// "SELL" when closing position; omit or "BUY" when opening.
    #[serde(skip_serializing_if = "Option::is_none")]
    order_type: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct PolymarketOrderResponse {
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    filled_size: Option<String>,
    #[serde(default)]
    avg_price: Option<String>,
    #[serde(default)]
    fee_paid: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct PolymarketOrderStatusResponse {
    #[serde(default)]
    order_id: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    filled_size: Option<String>,
    #[serde(default)]
    avg_price: Option<String>,
    #[serde(default)]
    fee_paid: Option<String>,
    #[serde(default)]
    fills: Option<Vec<PolymarketStatusFill>>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct PolymarketStatusFill {
    #[serde(default)]
    fill_id: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    trade_id: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    fee_paid: Option<String>,
}

#[derive(Clone, Debug)]
struct ActionExecutionResult {
    order_id: Option<String>,
    fills: Vec<ExecutionFill>,
    status: ExecutionStatus,
}

impl PolymarketHttpExecutionClient {
    pub fn new(api_base_url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            api_base_url: api_base_url.into(),
            api_key: None,
            funder: None,
            signature_type: None,
            per_order_max_retries: 0,
            per_order_backoff_ms: 50,
            status_poll_attempts: 3,
            status_poll_interval_ms: 500,
            nonce_counter: AtomicU64::new(1),
            signature_provider: None,
        }
    }

    pub fn with_api_key_env(mut self, env_key: &str) -> Self {
        self.api_key = env::var(env_key).ok();
        self
    }

    pub fn with_funder(mut self, funder: impl Into<String>) -> Self {
        let funder = funder.into().trim().to_string();
        if !funder.is_empty() {
            self.funder = Some(funder);
        }
        self
    }

    pub fn with_signature_type(mut self, signature_type: u8) -> Self {
        self.signature_type = Some(signature_type);
        self
    }

    pub fn api_base_url(&self) -> &str {
        &self.api_base_url
    }

    pub fn with_per_order_retry_policy(mut self, max_retries: u32, backoff_ms: u64) -> Self {
        self.per_order_max_retries = max_retries;
        self.per_order_backoff_ms = backoff_ms;
        self
    }

    pub fn with_status_polling(mut self, attempts: u32, interval_ms: u64) -> Self {
        self.status_poll_attempts = attempts;
        self.status_poll_interval_ms = interval_ms;
        self
    }

    pub fn with_signature_provider<F>(mut self, provider: F) -> Self
    where
        F: Fn(&OrderSignContext) -> Option<String> + Send + Sync + 'static,
    {
        self.signature_provider = Some(Arc::new(provider));
        self
    }

    pub fn with_static_signature_env(mut self, env_key: &str) -> Self {
        if let Ok(signature) = env::var(env_key) {
            self.signature_provider = Some(Arc::new(move |_ctx| Some(signature.clone())));
        }
        self
    }

    /// Loads a base64-url encoded CLOB secret from env and signs each request body dynamically.
    /// If decoding fails, falls back to the raw env value to preserve legacy static-signature behavior.
    pub fn with_dynamic_hmac_signature_env(mut self, env_key: &str) -> Self {
        let Ok(raw_value) = env::var(env_key) else {
            return self;
        };
        let trimmed = raw_value.trim().to_string();
        if trimmed.is_empty() {
            return self;
        }

        match URL_SAFE.decode(trimmed.as_bytes()) {
            Ok(decoded_secret) => {
                self.signature_provider = Some(Arc::new(move |ctx| {
                    let message = format!("{}{}{}{}", ctx.timestamp, ctx.method, ctx.path, ctx.body);
                    let mut mac = Hmac::<Sha256>::new_from_slice(&decoded_secret).ok()?;
                    mac.update(message.as_bytes());
                    let digest = mac.finalize().into_bytes();
                    Some(URL_SAFE.encode(digest))
                }));
            }
            Err(_) => {
                self.signature_provider = Some(Arc::new(move |_ctx| Some(trimmed.clone())));
            }
        }
        self
    }

    fn endpoint_url(&self, path: &str) -> String {
        let base = self.api_base_url.trim_end_matches('/');
        let p = path.trim_start_matches('/');
        format!("{}/{}", base, p)
    }

    fn action_to_request(
        &self,
        signal: &StrategySignal,
        action: &crate::types::OrderIntent,
    ) -> PolymarketOrderRequest {
        let side = match action.side {
            Side::Yes => "YES",
            Side::No => "NO",
        };
        let order_type = if action.sell {
            Some("SELL".to_string())
        } else {
            None
        };
        PolymarketOrderRequest {
            market_id: signal.market_id.clone(),
            side: side.to_string(),
            price: action.price.to_string(),
            size: action.size.to_string(),
            strategy_id: format!("{:?}", signal.strategy_id),
            order_type,
        }
    }

    fn parse_decimal_or(default: Decimal, value: Option<String>) -> Decimal {
        value
            .and_then(|v| Decimal::from_str(&v).ok())
            .unwrap_or(default)
    }

    fn summarize_status_fills(
        fills: &[PolymarketStatusFill],
        fallback_price: Decimal,
        fallback_fee_total: Decimal,
    ) -> (Decimal, Decimal, Decimal) {
        let mut total_size = Decimal::ZERO;
        let mut total_quote = Decimal::ZERO;
        let mut total_fee = Decimal::ZERO;

        for f in fills {
            let size = Self::parse_decimal_or(Decimal::ZERO, f.size.clone());
            let price = Self::parse_decimal_or(fallback_price, f.price.clone());
            let fee = Self::parse_decimal_or(Decimal::ZERO, f.fee_paid.clone());
            total_size += size;
            total_quote += size * price;
            total_fee += fee;
        }

        let avg_price = if total_size > Decimal::ZERO {
            total_quote / total_size
        } else {
            fallback_price
        };
        if total_fee <= Decimal::ZERO {
            total_fee = fallback_fee_total;
        }
        (total_size, avg_price, total_fee)
    }

    fn extract_status_fill_id(fill: &PolymarketStatusFill) -> Option<String> {
        fill.fill_id
            .clone()
            .or_else(|| fill.id.clone())
            .or_else(|| fill.trade_id.clone())
            .filter(|id| !id.trim().is_empty())
    }

    fn map_status(status: Option<String>, filled_size: Decimal, requested_size: Decimal) -> ExecutionStatus {
        if let Some(status) = status {
            match status.to_ascii_lowercase().as_str() {
                "filled" => return ExecutionStatus::Filled,
                "partial" | "partially_filled" | "partiallyfilled" => {
                    return ExecutionStatus::PartiallyFilled
                }
                "rejected" | "canceled" | "cancelled" => return ExecutionStatus::Rejected,
                "pending" | "open" | "new" => return ExecutionStatus::Pending,
                _ => {}
            }
        }
        if filled_size >= requested_size && requested_size > Decimal::ZERO {
            ExecutionStatus::Filled
        } else if filled_size > Decimal::ZERO {
            ExecutionStatus::PartiallyFilled
        } else {
            ExecutionStatus::Pending
        }
    }

    fn is_terminal(status: &ExecutionStatus) -> bool {
        matches!(status, ExecutionStatus::Filled | ExecutionStatus::Rejected)
    }

    fn extract_order_id(parsed: &PolymarketOrderResponse) -> Option<String> {
        parsed
            .order_id
            .clone()
            .or_else(|| parsed.id.clone())
            .filter(|id| !id.trim().is_empty())
    }

    fn next_nonce(&self) -> String {
        let n = self.nonce_counter.fetch_add(1, Ordering::Relaxed);
        format!("nonce-{n}")
    }

    async fn submit_single_action(
        &self,
        signal: &StrategySignal,
        action: crate::types::OrderIntent,
    ) -> Result<ActionExecutionResult, ExecutionError> {
        let mut attempts = 0_u32;
        loop {
            match self.try_submit_single_action(signal, action.clone()).await {
                Ok(v) => return Ok(v),
                Err(err) if attempts < self.per_order_max_retries => {
                    attempts += 1;
                    sleep(Duration::from_millis(self.per_order_backoff_ms)).await;
                    let _ = err;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn try_submit_single_action(
        &self,
        signal: &StrategySignal,
        action: crate::types::OrderIntent,
    ) -> Result<ActionExecutionResult, ExecutionError> {
        let payload = self.action_to_request(signal, &action);
        let ts = chrono::Utc::now().timestamp_millis().to_string();
        let nonce = self.next_nonce();
        let body = serde_json::to_string(&payload)
            .map_err(|e| ExecutionError::Backend(format!("failed to serialize order body: {e}")))?;
        let path = "/orders".to_string();
        let method = "POST".to_string();

        let mut req = self
            .http
            .post(self.endpoint_url(path.as_str()))
            .header("content-type", "application/json")
            .header("x-poly-timestamp", &ts)
            .header("x-poly-nonce", &nonce)
            .json(&payload);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        if let Some(signature_type) = self.signature_type {
            req = req.header("x-poly-signature-type", signature_type.to_string());
        }
        if let Some(funder) = &self.funder {
            req = req.header("x-poly-funder", funder);
        }
        if let Some(signature_provider) = &self.signature_provider {
            let sign_ctx = OrderSignContext {
                market_id: payload.market_id.clone(),
                side: payload.side.clone(),
                price: payload.price.clone(),
                size: payload.size.clone(),
                strategy_id: payload.strategy_id.clone(),
                timestamp: ts.clone(),
                nonce: nonce.clone(),
                method: method.clone(),
                path: path.clone(),
                body: body.clone(),
            };
            if let Some(signature) = signature_provider(&sign_ctx) {
                req = req.header("x-poly-signature", signature);
            }
        }

        let resp = req
            .send()
            .await
            .map_err(|e| ExecutionError::Backend(format!("http request failed: {e}")))?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ExecutionError::Backend(format!(
                "exchange rejected order: status={code}, body={body}"
            )));
        }

        let parsed = resp
            .json::<PolymarketOrderResponse>()
            .await
            .map_err(|e| ExecutionError::Backend(format!("invalid exchange response: {e}")))?;

        let order_id = Self::extract_order_id(&parsed);
        let filled_size = Self::parse_decimal_or(action.size, parsed.filled_size);
        let avg_price = Self::parse_decimal_or(action.price, parsed.avg_price);
        let fee_paid = Self::parse_decimal_or(Decimal::ZERO, parsed.fee_paid);
        let status = Self::map_status(parsed.status, filled_size, action.size);
        let mut out = ActionExecutionResult {
            order_id: order_id.clone(),
            fills: vec![ExecutionFill {
                order_id: order_id.clone(),
                fill_id: None,
                side: action.side.clone(),
                price: avg_price,
                size: filled_size,
                fee: fee_paid,
                sell: action.sell,
            }],
            status: status.clone(),
        };

        if let Some(id) = order_id {
            if !Self::is_terminal(&status) && self.status_poll_attempts > 0 {
                let polled = self.poll_order_status(&id, &action, filled_size, fee_paid).await?;
                out.fills.extend(polled.fills);
                out.status = polled.status;
            }
        }

        Ok(out)
    }

    async fn poll_order_status(
        &self,
        order_id: &str,
        action: &crate::types::OrderIntent,
        initial_filled: Decimal,
        initial_fee: Decimal,
    ) -> Result<ActionExecutionResult, ExecutionError> {
        let mut last_filled = initial_filled;
        let mut last_fee = initial_fee;
        let mut final_status = Self::map_status(None, last_filled, action.size);
        let mut delta_fills = Vec::new();
        let mut seen_fill_ids: HashSet<String> = HashSet::new();

        for _ in 0..self.status_poll_attempts {
            sleep(Duration::from_millis(self.status_poll_interval_ms)).await;
            let mut req = self
                .http
                .get(self.endpoint_url(&format!("orders/{order_id}")))
                .header("content-type", "application/json");
            if let Some(key) = &self.api_key {
                req = req.bearer_auth(key);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| ExecutionError::Backend(format!("status poll request failed: {e}")))?;
            if !resp.status().is_success() {
                let code = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ExecutionError::Backend(format!(
                    "status poll rejected: status={code}, body={body}"
                )));
            }

            let parsed = resp
                .json::<PolymarketOrderStatusResponse>()
                .await
                .map_err(|e| ExecutionError::Backend(format!("invalid status response: {e}")))?;

            let order_id_resolved = parsed
                .order_id
                .clone()
                .or_else(|| parsed.id.clone())
                .unwrap_or_else(|| order_id.to_string());
            let mut current_filled = Self::parse_decimal_or(last_filled, parsed.filled_size.clone());
            let mut current_avg_price = Self::parse_decimal_or(action.price, parsed.avg_price.clone());
            let mut current_fee_total = Self::parse_decimal_or(last_fee, parsed.fee_paid.clone());
            let mut emitted_from_ids = false;
            if let Some(status_fills) = parsed.fills.clone() {
                if !status_fills.is_empty() {
                    let (sum_size, avg_price, sum_fee) =
                        Self::summarize_status_fills(&status_fills, action.price, current_fee_total);
                    current_filled = sum_size;
                    current_avg_price = avg_price;
                    current_fee_total = sum_fee;

                    let all_have_ids = status_fills
                        .iter()
                        .all(|f| Self::extract_status_fill_id(f).is_some());
                    if all_have_ids {
                        emitted_from_ids = true;
                        for status_fill in status_fills {
                            let Some(fill_id) = Self::extract_status_fill_id(&status_fill) else {
                                continue;
                            };
                            if seen_fill_ids.insert(fill_id.clone()) {
                                let size = Self::parse_decimal_or(Decimal::ZERO, status_fill.size.clone());
                                if size <= Decimal::ZERO {
                                    continue;
                                }
                                let price =
                                    Self::parse_decimal_or(current_avg_price, status_fill.price.clone());
                                let fee =
                                    Self::parse_decimal_or(Decimal::ZERO, status_fill.fee_paid.clone());
                                delta_fills.push(ExecutionFill {
                                    order_id: Some(order_id_resolved.clone()),
                                    fill_id: Some(fill_id),
                                    side: action.side.clone(),
                                    price,
                                    size,
                                    fee,
                                    sell: action.sell,
                                });
                            }
                        }
                    }
                }
            }
            final_status = Self::map_status(parsed.status, current_filled, action.size);

            if !emitted_from_ids && current_filled > last_filled {
                let delta_size = current_filled - last_filled;
                let delta_fee = if current_fee_total > last_fee {
                    current_fee_total - last_fee
                } else {
                    Decimal::ZERO
                };
                delta_fills.push(ExecutionFill {
                    order_id: Some(order_id_resolved.clone()),
                    fill_id: None,
                    side: action.side.clone(),
                    price: current_avg_price,
                    size: delta_size,
                    fee: delta_fee,
                    sell: action.sell,
                });
                last_filled = current_filled;
                last_fee = current_fee_total;
            }

            if Self::is_terminal(&final_status) {
                break;
            }
        }

        Ok(ActionExecutionResult {
            order_id: Some(order_id.to_string()),
            fills: delta_fills,
            status: final_status,
        })
    }
}

#[async_trait]
impl ExecutionClient for PolymarketHttpExecutionClient {
    async fn submit(&self, signal: &StrategySignal) -> Result<ExecutionReport, ExecutionError> {
        let mut futures = signal
            .actions
            .iter()
            .cloned()
            .map(|action| self.submit_single_action(signal, action))
            .collect::<FuturesUnordered<_>>();

        let mut final_status = ExecutionStatus::Filled;
        let mut fills = Vec::new();
        let mut order_ids = Vec::new();

        while let Some(result) = futures.next().await {
            let action_result = result?;
            if let Some(order_id) = action_result.order_id {
                order_ids.push(order_id);
            }
            final_status = merge_status(final_status, action_result.status);
            fills.extend(action_result.fills);
        }

        Ok(ExecutionReport {
            market_id: signal.market_id.clone(),
            action_count: signal.actions.len(),
            order_ids,
            status: final_status,
            fills,
            signal: signal.clone(),
        })
    }
}

fn merge_status(lhs: ExecutionStatus, rhs: ExecutionStatus) -> ExecutionStatus {
    use ExecutionStatus::*;
    match (lhs, rhs) {
        (Rejected, _) | (_, Rejected) => Rejected,
        (PartiallyFilled, _) | (_, PartiallyFilled) => PartiallyFilled,
        (Pending, Filled) | (Filled, Pending) | (Pending, Pending) => Pending,
        (Filled, Filled) => Filled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::StrategyId;
    use crate::types::{OrderIntent, StrategySignal, StrategyState};

    #[test]
    fn summarize_status_fills_uses_fills_data() {
        let fills = vec![
            PolymarketStatusFill {
                fill_id: Some("f-1".to_string()),
                id: None,
                trade_id: None,
                price: Some("0.40".to_string()),
                size: Some("3".to_string()),
                fee_paid: Some("0.01".to_string()),
            },
            PolymarketStatusFill {
                fill_id: Some("f-2".to_string()),
                id: None,
                trade_id: None,
                price: Some("0.50".to_string()),
                size: Some("2".to_string()),
                fee_paid: Some("0.02".to_string()),
            },
        ];
        let (size, avg, fee) = PolymarketHttpExecutionClient::summarize_status_fills(
            &fills,
            Decimal::from_str("0.1").expect("invalid decimal"),
            Decimal::ZERO,
        );

        assert_eq!(size, Decimal::from_str("5").expect("invalid decimal"));
        assert_eq!(avg, Decimal::from_str("0.44").expect("invalid decimal"));
        assert_eq!(fee, Decimal::from_str("0.03").expect("invalid decimal"));
    }

    #[test]
    fn extract_status_fill_id_supports_multiple_fields() {
        let with_fill_id = PolymarketStatusFill {
            fill_id: Some("f1".to_string()),
            id: None,
            trade_id: None,
            price: None,
            size: None,
            fee_paid: None,
        };
        assert_eq!(
            PolymarketHttpExecutionClient::extract_status_fill_id(&with_fill_id),
            Some("f1".to_string())
        );

        let with_trade_id = PolymarketStatusFill {
            fill_id: None,
            id: None,
            trade_id: Some("t9".to_string()),
            price: None,
            size: None,
            fee_paid: None,
        };
        assert_eq!(
            PolymarketHttpExecutionClient::extract_status_fill_id(&with_trade_id),
            Some("t9".to_string())
        );
    }

    #[tokio::test]
    async fn eventing_execution_client_emits_fill_and_summary_events() {
        let inner = MockExecutionClient::default();
        let eventing = EventingExecutionClient::new(inner, 16);
        let mut rx = eventing.subscribe();

        let signal = StrategySignal {
            strategy_id: StrategyId::Arbitrage,
            market_id: "m-event".to_string(),
            actions: vec![
                OrderIntent {
                    side: Side::Yes,
                    price: Decimal::from_str("0.4").expect("invalid decimal"),
                    size: Decimal::from_str("1").expect("invalid decimal"),
                    sell: false,
                },
                OrderIntent {
                    side: Side::No,
                    price: Decimal::from_str("0.6").expect("invalid decimal"),
                    size: Decimal::from_str("1").expect("invalid decimal"),
                    sell: false,
                },
            ],
            state: StrategyState::Implemented,
        };

        let report = eventing.submit(&signal).await.expect("submit should succeed");
        assert_eq!(report.fills.len(), 2);

        let e1 = rx.recv().await.expect("first event");
        let e2 = rx.recv().await.expect("second event");
        let e3 = rx.recv().await.expect("summary event");
        assert!(e1.event_id < e2.event_id && e2.event_id < e3.event_id);
        assert!(e1.occurred_at <= e2.occurred_at && e2.occurred_at <= e3.occurred_at);
        assert_eq!(e1.event_type, ExecutionEventType::FillDelta);
        assert_eq!(e2.event_type, ExecutionEventType::FillDelta);
        assert_eq!(e3.event_type, ExecutionEventType::ReportSummary);
        assert!(e1.error.is_none() && e2.error.is_none() && e3.error.is_none());
        let fill_events = [e1.fill.is_some(), e2.fill.is_some(), e3.fill.is_some()]
            .iter()
            .filter(|x| **x)
            .count();
        assert_eq!(fill_events, 2);
    }

    #[tokio::test]
    async fn eventing_execution_client_emits_error_event() {
        let inner = FlakyExecutionClient::new(1);
        let eventing = EventingExecutionClient::new(inner, 8);
        let mut rx = eventing.subscribe();

        let signal = StrategySignal {
            strategy_id: StrategyId::Arbitrage,
            market_id: "m-err".to_string(),
            actions: vec![OrderIntent {
                side: Side::Yes,
                price: Decimal::from_str("0.4").expect("invalid decimal"),
                size: Decimal::from_str("1").expect("invalid decimal"),
                sell: false,
            }],
            state: StrategyState::Implemented,
        };

        let err = eventing
            .submit(&signal)
            .await
            .expect_err("submit should fail once");
        assert!(err.to_string().contains("simulated failure"));

        let evt = rx.recv().await.expect("error event should be emitted");
        assert_eq!(evt.event_type, ExecutionEventType::Error);
        assert!(evt.error.is_some());
        assert!(evt.fill.is_none());
    }
}
