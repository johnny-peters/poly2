use std::fs;
use std::path::Path;

use poly2::{AppConfig, HealthcheckReport, CheckItem};

#[test]
fn load_default_config_and_parse_runtime_sections() {
    let path = Path::new("config/default.yaml");
    let cfg = AppConfig::load_from_path_with_env_overlay(path, None::<&str>)
        .expect("should load yaml config");

    assert!(cfg.strategies.arbitrage.enabled);
    assert!(!cfg.strategies.market_making.enabled);

    let arbi = cfg.arbitrage_config().expect("arbitrage config should parse");
    assert!(arbi.min_profit_threshold > rust_decimal::Decimal::ZERO);

    let risk = cfg.risk_config().expect("risk config should parse");
    assert!(risk.max_position_pct > rust_decimal::Decimal::ZERO);

    let execution = cfg.execution_settings();
    assert_eq!(execution.max_retries, 3);
    assert!(execution.polymarket_http_url.is_some());
    assert!(execution.signature_env.is_some());
    assert_eq!(execution.status_poll_attempts, 3);
    assert_eq!(execution.status_poll_interval_ms, 500);
}

#[test]
fn dotenv_overlay_overrides_execution_settings() {
    let temp_path = std::env::temp_dir().join("poly2-config-test.env");
    fs::write(
        &temp_path,
        "CLOB_HTTP_URL='https://overlay-http.example'\nCLOB_WS_URL='wss://overlay-ws.example'\nNETWORK_RETRY_LIMIT=9\nREQUEST_TIMEOUT_MS=2500\nSTATUS_POLL_INTERVAL_MS=2000\n",
    )
    .expect("should write temp env file");

    let cfg = AppConfig::load_from_path_with_env_overlay(
        "config/default.yaml",
        Some(temp_path.as_path()),
    )
    .expect("should load config with overlay");

    let exec = cfg.execution_settings();
    assert_eq!(exec.polymarket_http_url.as_deref(), Some("https://overlay-http.example"));
    assert_eq!(exec.polymarket_ws_url, "wss://overlay-ws.example");
    assert_eq!(exec.max_retries, 9);
    assert_eq!(exec.timeout_secs, 3);
    assert_eq!(exec.status_poll_interval_ms, 2000);

    let _ = fs::remove_file(temp_path);
}

#[test]
fn healthcheck_report_passed_aggregates_statuses() {
    let report = HealthcheckReport {
        checks: vec![
            CheckItem {
                name: "a".to_string(),
                ok: true,
                detail: "ok".to_string(),
            },
            CheckItem {
                name: "b".to_string(),
                ok: false,
                detail: "fail".to_string(),
            },
        ],
    };
    assert!(!report.passed());
}
