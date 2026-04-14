use std::fs;
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::risk::RiskConfig;
use crate::strategy::ArbitrageConfig;

#[derive(Clone, Debug, Deserialize)]
pub struct StrategyToggleConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ArbitrageFileConfig {
    pub enabled: bool,
    pub min_profit_threshold: String,
    pub slippage_bps: u32,
    pub min_liquidity: String,
    pub order_size: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StrategiesFileConfig {
    pub arbitrage: ArbitrageFileConfig,
    pub probability_trading: StrategyToggleConfig,
    pub market_making: StrategyToggleConfig,
    pub latency_arbitrage: StrategyToggleConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RiskFileConfig {
    pub max_position_pct: String,
    pub max_single_trade_pct: String,
    pub daily_loss_limit_pct: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExecutionFileConfig {
    pub polymarket_ws_url: String,
    pub polymarket_http_url: Option<String>,
    pub api_key_env: Option<String>,
    pub signature_env: Option<String>,
    pub max_retries: u32,
    pub timeout_secs: u64,
    #[serde(default = "default_status_poll_attempts")]
    pub status_poll_attempts: u32,
    #[serde(default = "default_status_poll_interval_ms")]
    pub status_poll_interval_ms: u64,
    #[serde(default = "default_scan_min_minutes_to_settle")]
    pub scan_min_minutes_to_settle: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    pub strategies: StrategiesFileConfig,
    pub risk: RiskFileConfig,
    pub execution: ExecutionFileConfig,
}

#[derive(Clone, Debug)]
pub struct ExecutionSettings {
    pub polymarket_ws_url: String,
    pub polymarket_http_url: Option<String>,
    pub api_key_env: Option<String>,
    pub signature_env: Option<String>,
    pub max_retries: u32,
    pub timeout_secs: u64,
    pub status_poll_attempts: u32,
    pub status_poll_interval_ms: u64,
    pub scan_min_minutes_to_settle: u64,
}

fn default_status_poll_attempts() -> u32 {
    3
}

fn default_status_poll_interval_ms() -> u64 {
    500
}

fn default_scan_min_minutes_to_settle() -> u64 {
    30
}

impl AppConfig {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path_ref = path.as_ref();
        let raw = fs::read_to_string(path_ref)
            .with_context(|| format!("failed to read config file: {}", path_ref.display()))?;
        serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse yaml config: {}", path_ref.display()))
    }

    pub fn load_from_path_with_env_overlay(
        yaml_path: impl AsRef<Path>,
        dotenv_path: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let mut cfg = Self::load_from_path(yaml_path)?;
        let dotenv_map = if let Some(path) = dotenv_path {
            load_dotenv_map(path.as_ref())?
        } else {
            Default::default()
        };
        cfg.apply_env_overlay(&dotenv_map);
        Ok(cfg)
    }

    pub fn arbitrage_config(&self) -> Result<ArbitrageConfig> {
        let min_profit_threshold = Decimal::from_str(&self.strategies.arbitrage.min_profit_threshold)
            .context("invalid strategies.arbitrage.min_profit_threshold")?;
        let slippage_bps = Decimal::from(self.strategies.arbitrage.slippage_bps);
        let slippage_ratio = slippage_bps / Decimal::from(10_000_u32);
        let min_liquidity = Decimal::from_str(&self.strategies.arbitrage.min_liquidity)
            .context("invalid strategies.arbitrage.min_liquidity")?;
        let order_size = Decimal::from_str(&self.strategies.arbitrage.order_size)
            .context("invalid strategies.arbitrage.order_size")?;

        Ok(ArbitrageConfig::simple(
            min_profit_threshold,
            slippage_ratio,
            min_liquidity,
            order_size,
        ))
    }

    pub fn risk_config(&self) -> Result<RiskConfig> {
        Ok(RiskConfig {
            max_position_pct: Decimal::from_str(&self.risk.max_position_pct)
                .context("invalid risk.max_position_pct")?,
            max_single_trade_pct: Decimal::from_str(&self.risk.max_single_trade_pct)
                .context("invalid risk.max_single_trade_pct")?,
            daily_loss_limit_pct: Decimal::from_str(&self.risk.daily_loss_limit_pct)
                .context("invalid risk.daily_loss_limit_pct")?,
        })
    }

    pub fn execution_settings(&self) -> ExecutionSettings {
        ExecutionSettings {
            polymarket_ws_url: self.execution.polymarket_ws_url.clone(),
            polymarket_http_url: self.execution.polymarket_http_url.clone(),
            api_key_env: self.execution.api_key_env.clone(),
            signature_env: self.execution.signature_env.clone(),
            max_retries: self.execution.max_retries,
            timeout_secs: self.execution.timeout_secs,
            status_poll_attempts: self.execution.status_poll_attempts,
            status_poll_interval_ms: self.execution.status_poll_interval_ms,
            scan_min_minutes_to_settle: self.execution.scan_min_minutes_to_settle,
        }
    }

    fn apply_env_overlay(&mut self, dotenv_map: &std::collections::HashMap<String, String>) {
        if let Some(v) = resolve_override("CLOB_WS_URL", dotenv_map) {
            self.execution.polymarket_ws_url = v;
        }
        if let Some(v) = resolve_override("CLOB_HTTP_URL", dotenv_map) {
            self.execution.polymarket_http_url = Some(v);
        }
        if let Some(v) = resolve_override("NETWORK_RETRY_LIMIT", dotenv_map)
            .or_else(|| resolve_override("RETRY_LIMIT", dotenv_map))
        {
            if let Ok(parsed) = v.parse::<u32>() {
                self.execution.max_retries = parsed;
            }
        }
        if let Some(v) = resolve_override("REQUEST_TIMEOUT_MS", dotenv_map) {
            if let Ok(ms) = v.parse::<u64>() {
                self.execution.timeout_secs = ms.div_ceil(1000).max(1);
            }
        }
        if let Some(v) = resolve_override("STATUS_POLL_INTERVAL_MS", dotenv_map) {
            if let Ok(ms) = v.parse::<u64>() {
                self.execution.status_poll_interval_ms = ms.max(1);
            }
        }
        if let Some(v) = resolve_override("SCAN_MIN_MINUTES_TO_SETTLE", dotenv_map) {
            if let Ok(minutes) = v.parse::<u64>() {
                self.execution.scan_min_minutes_to_settle = minutes;
            }
        }
    }
}

fn resolve_override(
    key: &str,
    dotenv_map: &std::collections::HashMap<String, String>,
) -> Option<String> {
    std::env::var(key).ok().or_else(|| dotenv_map.get(key).cloned())
}

fn load_dotenv_map(path: &Path) -> Result<std::collections::HashMap<String, String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read dotenv file: {}", path.display()))?;
    let mut out = std::collections::HashMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((k, v)) = trimmed.split_once('=') else {
            continue;
        };
        let key = k.trim().to_string();
        if key.is_empty() {
            continue;
        }
        let mut val = v.trim().to_string();
        if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
        {
            val = val[1..val.len() - 1].to_string();
        } else if let Some((head, _)) = val.split_once('#') {
            val = head.trim().to_string();
        }
        out.insert(key, val);
    }
    Ok(out)
}
