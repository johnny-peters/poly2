//! Plan C scoring IC back-test harness.
//!
//! This example measures the **Spearman rank IC** of a leaderboard scoring
//! function against realised **forward 7d PnL / ROI** on a sample of leader
//! snapshots, and prints a head-to-head comparison of the legacy v1 score
//! (raw weighted pnl/vol/interval/profit-factor/recent-roi, no drawdown
//! penalty, no Bayesian shrinkage, no Wilson LCB) versus the upgraded v2
//! score implemented in `src/plan_c.rs`.
//!
//! Rationale — the upstream upgrade plan's acceptance criterion requires
//! `IC_v2 ≥ IC_v1` on historical data; this harness makes that check a
//! single-command regression rather than a manual exercise.
//!
//! ## Usage
//!
//! ### 1. Synthetic demo (no CSV needed)
//!
//! ```bash
//! cargo run --example plan_c_ic_backtest
//! ```
//!
//! Generates 400 synthetic leaders with a latent "edge" parameter, injects
//! noise into their metrics, and verifies IC_v2 > IC_v1. Serves as smoke test.
//!
//! ### 2. Real historical snapshots
//!
//! ```bash
//! cargo run --example plan_c_ic_backtest -- path/to/snapshots.csv
//! ```
//!
//! CSV schema (header row required; extra columns are ignored):
//!
//! | column             | meaning                                                |
//! | ------------------ | ------------------------------------------------------ |
//! | `wallet`           | proxy wallet address (for logging only)                |
//! | `sharpe_7d`        | Sharpe-like ratio from daily realised PnL              |
//! | `wilson_wr`        | Wilson 95% LCB of win-rate                             |
//! | `pf_shrunk`        | Bayesian-shrunk profit factor (alpha=beta=50)          |
//! | `recent_roi`       | realised pnl / buy-notional in `recent_window_hours`   |
//! | `dd_penalty`       | max_drawdown / max(|net_realized|, 1), clamped [0,1]   |
//! | `wins_raw`         | raw win count (for legacy v1 pnl/vol/WR mix)           |
//! | `closed_raw`       | raw closed trades count                                |
//! | `pnl_total`        | total realised pnl over look-back                      |
//! | `volume_total`     | total volume over look-back                            |
//! | `profit_factor_raw`| unshrunk profit factor (gp/gl, 999 if gl≈0)           |
//! | `fwd_pnl_7d`       | **out-of-sample** realised PnL in next 7 days          |
//! | `fwd_roi_7d`       | **out-of-sample** ROI (`fwd_pnl_7d / deployed_capital`)|
//!
//! The harness prints IC vs `fwd_pnl_7d` and IC vs `fwd_roi_7d` for both
//! scoring functions, and the delta `IC_v2 - IC_v1`.

use std::env;
use std::fs;
use std::process::ExitCode;

// ------------------------------------------------------------------
// Pure scoring primitives — duplicated from `src/plan_c.rs` so this
// example can run without touching the private module tree. KEEP IN
// SYNC with plan_c.rs; the tests at the bottom of plan_c.rs anchor
// their behaviour.
// ------------------------------------------------------------------

fn retention_norm(x: f64, lo: f64, hi: f64) -> f64 {
    if (hi - lo).abs() < 1e-12 {
        return 0.0;
    }
    ((x - lo) / (hi - lo)).clamp(0.0, 1.0)
}

fn wilson_lower_bound(wins: usize, n: usize, z: f64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let p = wins as f64 / n as f64;
    let n_f = n as f64;
    let denom = 1.0 + z * z / n_f;
    let centre = p + z * z / (2.0 * n_f);
    let margin = z * ((p * (1.0 - p) + z * z / (4.0 * n_f)) / n_f).sqrt();
    ((centre - margin) / denom).max(0.0)
}

fn profit_factor_shrinkage(gp: f64, gl: f64, alpha: f64, beta: f64) -> f64 {
    let num = gp + alpha;
    let den = gl + beta;
    if den <= 0.0 {
        0.0
    } else {
        num / den
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_score_v2(
    sharpe_7d: f64,
    wilson_wr: f64,
    profit_factor_shrunk: f64,
    recent_roi: f64,
    dd_penalty: f64,
    w_ret: f64,
    w_wr: f64,
    w_pf: f64,
    w_rr: f64,
    w_dd: f64,
) -> f64 {
    let ret_norm = retention_norm(sharpe_7d.clamp(-3.0, 6.0), -3.0, 6.0);
    let pf_norm = profit_factor_shrunk.clamp(0.0, 8.0) / 8.0;
    let rr_norm = retention_norm(recent_roi.clamp(-0.75, 1.5), -0.75, 1.5);
    w_ret * ret_norm
        + w_wr * wilson_wr.clamp(0.0, 1.0)
        + w_pf * pf_norm
        + w_rr * rr_norm
        - w_dd * dd_penalty.clamp(0.0, 1.0)
}

/// Legacy "v1" score — a reconstruction of what Plan C used before the
/// upgrade: no Wilson LCB, no Bayesian PF shrinkage, no drawdown penalty.
/// Used only inside this harness as the baseline in the IC comparison.
fn compute_score_v1(
    pnl: f64,
    volume: f64,
    wins_raw: usize,
    closed_raw: usize,
    profit_factor_raw: f64,
    recent_roi: f64,
) -> f64 {
    let roi = if volume > 0.0 { pnl / volume } else { 0.0 };
    let roi_norm = retention_norm(roi.clamp(-0.5, 0.5), -0.5, 0.5);
    let wr_raw = if closed_raw == 0 {
        0.0
    } else {
        wins_raw as f64 / closed_raw as f64
    };
    let pf_norm = profit_factor_raw.clamp(0.0, 8.0) / 8.0;
    let rr_norm = retention_norm(recent_roi.clamp(-0.75, 1.5), -0.75, 1.5);
    0.30 * roi_norm + 0.30 * wr_raw + 0.25 * pf_norm + 0.15 * rr_norm
}

// ------------------------------------------------------------------
// Spearman rank correlation (= Pearson correlation of ranks).
// Ties broken by midrank. Returns NaN for degenerate inputs.
// ------------------------------------------------------------------

fn ranks(xs: &[f64]) -> Vec<f64> {
    let mut idx: Vec<usize> = (0..xs.len()).collect();
    idx.sort_by(|a, b| xs[*a].partial_cmp(&xs[*b]).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranks = vec![0.0; xs.len()];
    let mut i = 0;
    while i < idx.len() {
        let mut j = i + 1;
        while j < idx.len() && (xs[idx[j]] - xs[idx[i]]).abs() < 1e-12 {
            j += 1;
        }
        // Tied block [i, j) -> share midrank (i + j - 1) / 2 + 1 (1-indexed).
        let mid = (i + j - 1) as f64 / 2.0 + 1.0;
        for k in i..j {
            ranks[idx[k]] = mid;
        }
        i = j;
    }
    ranks
}

fn pearson(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len();
    if n == 0 || ys.len() != n {
        return f64::NAN;
    }
    let mx: f64 = xs.iter().sum::<f64>() / n as f64;
    let my: f64 = ys.iter().sum::<f64>() / n as f64;
    let mut num = 0.0;
    let mut vx = 0.0;
    let mut vy = 0.0;
    for i in 0..n {
        let dx = xs[i] - mx;
        let dy = ys[i] - my;
        num += dx * dy;
        vx += dx * dx;
        vy += dy * dy;
    }
    if vx <= 0.0 || vy <= 0.0 {
        return f64::NAN;
    }
    num / (vx * vy).sqrt()
}

fn spearman(xs: &[f64], ys: &[f64]) -> f64 {
    pearson(&ranks(xs), &ranks(ys))
}

// ------------------------------------------------------------------
// CSV loading + IC report
// ------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
struct Row {
    wallet: String,
    sharpe_7d: f64,
    wilson_wr: f64,
    pf_shrunk: f64,
    recent_roi: f64,
    dd_penalty: f64,
    wins_raw: f64,
    closed_raw: f64,
    pnl_total: f64,
    volume_total: f64,
    profit_factor_raw: f64,
    fwd_pnl_7d: f64,
    fwd_roi_7d: f64,
}

fn parse_csv(text: &str) -> anyhow::Result<Vec<Row>> {
    let mut lines = text.lines();
    let header = lines.next().ok_or_else(|| anyhow::anyhow!("empty CSV"))?;
    let cols: Vec<&str> = header.split(',').map(str::trim).collect();
    let get = |name: &str| cols.iter().position(|c| c.eq_ignore_ascii_case(name));

    let idx_wallet = get("wallet");
    let idx_sharpe = get("sharpe_7d");
    let idx_wr = get("wilson_wr");
    let idx_pf_s = get("pf_shrunk");
    let idx_rr = get("recent_roi");
    let idx_dd = get("dd_penalty");
    let idx_wins = get("wins_raw");
    let idx_closed = get("closed_raw");
    let idx_pnl = get("pnl_total");
    let idx_vol = get("volume_total");
    let idx_pf_raw = get("profit_factor_raw");
    let idx_fwd_pnl = get("fwd_pnl_7d");
    let idx_fwd_roi = get("fwd_roi_7d");

    let pick = |row: &[&str], idx: Option<usize>| -> f64 {
        idx.and_then(|i| row.get(i))
            .and_then(|s| s.trim().parse::<f64>().ok())
            .unwrap_or(0.0)
    };

    let mut out = Vec::new();
    for (lineno, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        let row = Row {
            wallet: idx_wallet
                .and_then(|i| fields.get(i))
                .unwrap_or(&"")
                .to_string(),
            sharpe_7d: pick(&fields, idx_sharpe),
            wilson_wr: pick(&fields, idx_wr),
            pf_shrunk: pick(&fields, idx_pf_s),
            recent_roi: pick(&fields, idx_rr),
            dd_penalty: pick(&fields, idx_dd),
            wins_raw: pick(&fields, idx_wins),
            closed_raw: pick(&fields, idx_closed),
            pnl_total: pick(&fields, idx_pnl),
            volume_total: pick(&fields, idx_vol),
            profit_factor_raw: pick(&fields, idx_pf_raw),
            fwd_pnl_7d: pick(&fields, idx_fwd_pnl),
            fwd_roi_7d: pick(&fields, idx_fwd_roi),
        };
        if row.fwd_pnl_7d.is_nan() {
            anyhow::bail!("CSV line {}: non-numeric fwd_pnl_7d", lineno + 2);
        }
        out.push(row);
    }
    Ok(out)
}

fn score_rows(rows: &[Row]) -> (Vec<f64>, Vec<f64>) {
    let v1: Vec<f64> = rows
        .iter()
        .map(|r| {
            compute_score_v1(
                r.pnl_total,
                r.volume_total,
                r.wins_raw as usize,
                r.closed_raw as usize,
                r.profit_factor_raw,
                r.recent_roi,
            )
        })
        .collect();
    let v2: Vec<f64> = rows
        .iter()
        .map(|r| {
            compute_score_v2(
                r.sharpe_7d,
                r.wilson_wr,
                r.pf_shrunk,
                r.recent_roi,
                r.dd_penalty,
                0.25, // w_ret
                0.25, // w_wr
                0.20, // w_pf
                0.15, // w_rr
                0.15, // w_dd
            )
        })
        .collect();
    (v1, v2)
}

fn report(label: &str, rows: &[Row]) {
    let (v1, v2) = score_rows(rows);
    let fwd_pnl: Vec<f64> = rows.iter().map(|r| r.fwd_pnl_7d).collect();
    let fwd_roi: Vec<f64> = rows.iter().map(|r| r.fwd_roi_7d).collect();

    let ic_v1_pnl = spearman(&v1, &fwd_pnl);
    let ic_v2_pnl = spearman(&v2, &fwd_pnl);
    let ic_v1_roi = spearman(&v1, &fwd_roi);
    let ic_v2_roi = spearman(&v2, &fwd_roi);

    println!("\n=== IC report: {label} (N={}) ===", rows.len());
    println!(
        "  vs fwd_pnl_7d:  IC_v1={ic_v1_pnl:>+.4}   IC_v2={ic_v2_pnl:>+.4}   \u{0394}={:+.4}",
        ic_v2_pnl - ic_v1_pnl
    );
    println!(
        "  vs fwd_roi_7d:  IC_v1={ic_v1_roi:>+.4}   IC_v2={ic_v2_roi:>+.4}   \u{0394}={:+.4}",
        ic_v2_roi - ic_v1_roi
    );
    if ic_v2_pnl >= ic_v1_pnl && ic_v2_roi >= ic_v1_roi {
        println!("  PASS — v2 \u{2265} v1 on both targets.");
    } else {
        println!("  WARN — v2 underperforms v1 on at least one target.");
    }
}

// ------------------------------------------------------------------
// Synthetic generator for smoke-testing the harness.
//
// Model: each leader has a latent scalar "edge" ~ N(0, 1). Their observed
// metrics are noisy projections of edge; forward pnl is (edge - dd) with
// multiplicative noise. v2 uses wilson_wr + pf_shrunk + dd penalty, which
// are closer to edge than v1's raw pnl/vol mix — so IC_v2 should win.
// Reproducible via a tiny LCG so CI behaviour is stable.
// ------------------------------------------------------------------

struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next_f64(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 11) as f64) / ((1u64 << 53) as f64)
    }
    fn normal(&mut self) -> f64 {
        // Box–Muller
        let u1 = (self.next_f64()).max(1e-12);
        let u2 = self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

fn synthetic(n: usize, seed: u64) -> Vec<Row> {
    let mut rng = Lcg::new(seed);
    (0..n)
        .map(|i| {
            let edge = rng.normal();
            let mut noise = |s: f64| s * rng.normal();

            // Leaders with bigger edge tend to:
            //   * take more closed trades (better sample size for WR / PF)
            //   * post higher sharpe_7d
            //   * have smaller drawdowns
            let closed = ((edge * 30.0 + noise(10.0)) + 60.0).max(5.0);
            let wr_true = (0.5 + 0.08 * edge).clamp(0.05, 0.95);
            let wins = (closed * wr_true + noise(1.5)).clamp(0.0, closed);
            let pnl = edge * 5000.0 + noise(1500.0);
            let volume = (edge.abs() + 1.0) * 50000.0 + noise(10000.0).abs();
            let pf_raw = (1.0 + 0.7 * edge + noise(0.3)).clamp(0.1, 8.0);
            let pf_s = profit_factor_shrinkage(
                (pf_raw - 1.0).max(0.0) * 1000.0,
                1000.0_f64.max(1.0),
                50.0,
                50.0,
            );
            let wilson = wilson_lower_bound(wins as usize, closed as usize, 1.96);
            let sharpe = (edge * 1.2 + noise(0.5)).clamp(-3.0, 6.0);
            let recent_roi = (edge * 0.15 + noise(0.08)).clamp(-0.75, 1.5);
            let dd_penalty = ((-edge * 0.2) + 0.35 + noise(0.1)).clamp(0.0, 1.0);

            // The forward target depends dominantly on edge but is penalised
            // by realised drawdown — exactly the signal v2 adds.
            let fwd_pnl_7d = (edge - 0.6 * dd_penalty) * 2000.0 + noise(800.0);
            let fwd_roi_7d = fwd_pnl_7d / volume.max(1.0);

            Row {
                wallet: format!("0xsynth{i:05}"),
                sharpe_7d: sharpe,
                wilson_wr: wilson,
                pf_shrunk: pf_s,
                recent_roi,
                dd_penalty,
                wins_raw: wins,
                closed_raw: closed,
                pnl_total: pnl,
                volume_total: volume,
                profit_factor_raw: pf_raw,
                fwd_pnl_7d,
                fwd_roi_7d,
            }
        })
        .collect()
}

// ------------------------------------------------------------------

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None => {
            println!(
                "[synthetic] no CSV path given; generating 400 synthetic leaders (seed=7)."
            );
            let rows = synthetic(400, 7);
            report("synthetic N=400, seed=7", &rows);
        }
        Some(path) => {
            let text = match fs::read_to_string(path) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("failed to read {path}: {e}");
                    return ExitCode::from(2);
                }
            };
            let rows = match parse_csv(&text) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to parse CSV: {e}");
                    return ExitCode::from(2);
                }
            };
            if rows.is_empty() {
                eprintln!("CSV had header but no data rows.");
                return ExitCode::from(2);
            }
            report(&format!("file: {path}"), &rows);
        }
    }
    ExitCode::SUCCESS
}
