//! On-chain redeem support for Plan C copy-trading positions.
//!
//! When a Polymarket market resolves, the bot's CLOB-based exit path can no
//! longer close the position (the orderbook is gone). This module calls the
//! Gnosis ConditionalTokens contract (or Polymarket's `NegRiskAdapter` for
//! neg-risk markets) to claim settlement collateral, freeing the position so
//! it stops occupying a slot in `PLAN_C_MAX_POSITIONS`.
//!
//! Two supported redeem paths:
//!
//! - **Standard binary markets**: call
//!   `ConditionalTokens.redeemPositions(collateral, 0x0, conditionId, [1, 2])`.
//!   The contract auto-redeems the caller's full balance for the YES and NO
//!   outcomes and transfers `collateral` back. After the 2026-04-28 V2 cutover
//!   `collateral` is **pUSD** (Polymarket USD), not USDC.e. Positions opened
//!   pre-cutover are still anchored on USDC.e — operators redeeming legacy
//!   positions must override `PLAN_C_COLLATERAL_ADDRESS` (or the deprecated
//!   alias `PLAN_C_USDC_ADDRESS`) back to the USDC.e address.
//!
//! - **NegRisk multi-outcome markets**: call
//!   `NegRiskAdapter.redeemPositions(conditionId, [yes_bal, no_bal])`.
//!   The adapter unwraps `WrappedCollateral` → collateral (pUSD post-V2,
//!   USDC.e pre-V2) for us. Because the adapter transfers the requested
//!   amount from `msg.sender`, we must query the caller's ERC1155 balance
//!   for both outcome position IDs first.

use alloy::{
    network::EthereumWallet,
    primitives::{Address, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::{anyhow, Context};
use std::str::FromStr;

sol! {
    #[sol(rpc)]
    interface IConditionalTokens {
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        ) external;

        function payoutDenominator(bytes32 conditionId) external view returns (uint256);

        function balanceOf(address owner, uint256 id) external view returns (uint256);

        function getCollectionId(
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256 indexSet
        ) external view returns (bytes32);

        function getPositionId(
            address collateralToken,
            bytes32 collectionId
        ) external view returns (uint256);
    }

    #[sol(rpc)]
    interface INegRiskAdapter {
        function redeemPositions(bytes32 conditionId, uint256[] amounts) external;
        function wcol() external view returns (address);
    }
}

/// Polymarket contract addresses on Polygon mainnet. Override via env if
/// Polymarket ever redeploys. Source of truth:
/// <https://docs.polymarket.com/resources/contracts>.
pub const DEFAULT_CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
/// Legacy NegRiskAdapter still listed under "Core Trading Contracts" after the
/// V2 cutover. Used by `redeem_neg_risk` for the on-chain unwrap call.
pub const DEFAULT_NEG_RISK_ADAPTER: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";
/// Default collateral token = **pUSD** (Polymarket USD) post 2026-04-28 V2
/// cutover. Backed 1:1 by USDC, used for all CLOB settlement on V2.
/// Pre-cutover positions are still anchored on USDC.e — see
/// `LEGACY_USDCE_ADDRESS` for that.
pub const DEFAULT_COLLATERAL_ADDRESS: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
/// Bridged USDC.e on Polygon — V1 collateral, only used by post-cutover
/// startup checks ("you have unwrapped USDC.e — please wrap to pUSD") and by
/// operators redeeming legacy positions opened before the cutover.
pub const LEGACY_USDCE_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
/// V2 CollateralOnramp — wraps USDC / USDC.e into pUSD via `wrap()`. We never
/// call this from the bot (operators wrap manually on polymarket.com), but
/// log the address whenever we detect unwrapped USDC.e on the funder wallet.
pub const COLLATERAL_ONRAMP_ADDRESS: &str = "0x93070a847efEf7F70739046A929D47a521F5B8ee";
/// V2 CtfCollateralAdapter — bridges pUSD ↔ CTF operations (split/merge/redeem)
/// for non-NegRisk markets. Reserved for future use; current redeem path uses
/// the standard `ConditionalTokens.redeemPositions(pUSD, …)`, which still
/// works post-cutover.
#[allow(dead_code)]
pub const CTF_COLLATERAL_ADAPTER_ADDRESS: &str = "0xADa100874d00e3331D00F2007a9c336a65009718";
/// V2 NegRiskCtfCollateralAdapter — pUSD-aware variant for NegRisk markets.
/// Reserved for future use; current redeem path still goes through
/// `DEFAULT_NEG_RISK_ADAPTER` (`0xd91E80…`).
#[allow(dead_code)]
pub const NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS: &str =
    "0xAdA200001000ef00D07553cEE7006808F895c6F1";
pub const DEFAULT_RPC_URL: &str = "https://polygon-rpc.com";

/// Backwards-compat alias kept so callers that still reference
/// `DEFAULT_USDC_ADDRESS` keep compiling. Points at pUSD post-V2 cutover.
#[allow(dead_code)]
#[deprecated(note = "renamed to DEFAULT_COLLATERAL_ADDRESS (pUSD post 2026-04-28 V2)")]
pub const DEFAULT_USDC_ADDRESS: &str = DEFAULT_COLLATERAL_ADDRESS;

/// All the state we need to perform redemptions from a single wallet.
pub struct RedeemClient {
    provider: DynProvider,
    /// Either the EOA (Safe-less mode) or the Polymarket proxy wallet — whoever
    /// actually holds the CTF position tokens. For most Polymarket users this
    /// is the proxy wallet; the EOA only signs.
    position_holder: Address,
    ctf_address: Address,
    neg_risk_adapter: Address,
    /// Settlement collateral token. Post 2026-04-28 V2 cutover this is pUSD
    /// (`0xC011a7E1…`); pre-cutover positions used USDC.e (`0x2791Bca1…`).
    /// The CTF identifies positions by `(collateral, collectionId)`, so this
    /// must match the collateral the position was originally minted with.
    collateral_address: Address,
    /// WrappedCollateral address used by neg-risk positions. Looked up from
    /// the adapter at construction time so we don't have to hardcode it.
    wcol_address: Address,
}

impl RedeemClient {
    pub async fn new(
        rpc_url: &str,
        private_key: &str,
        position_holder: Address,
        ctf_address: Address,
        neg_risk_adapter: Address,
        collateral_address: Address,
    ) -> anyhow::Result<Self> {
        let signer = PrivateKeySigner::from_str(private_key.trim())
            .context("invalid PLAN_C_REDEEM private key")?;
        let wallet = EthereumWallet::from(signer);

        let url = rpc_url
            .parse()
            .with_context(|| format!("invalid PLAN_C_RPC_URL: {rpc_url}"))?;
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(url)
            .erased();

        // Look up WrappedCollateral address from the adapter. This avoids a
        // hard-coded value and will automatically pick up any redeployment.
        let adapter = INegRiskAdapter::new(neg_risk_adapter, &provider);
        let wcol_address = adapter
            .wcol()
            .call()
            .await
            .with_context(|| format!("NegRiskAdapter.wcol() failed at {}", neg_risk_adapter))?;

        Ok(Self {
            provider,
            position_holder,
            ctf_address,
            neg_risk_adapter,
            collateral_address,
            wcol_address,
        })
    }

    /// Returns true iff the condition has been reported on-chain (i.e.
    /// payoutDenominator > 0). Does not check whether the caller actually
    /// holds any positions.
    pub async fn is_resolved(&self, condition_id: B256) -> anyhow::Result<bool> {
        let ctf = IConditionalTokens::new(self.ctf_address, &self.provider);
        let denom = ctf.payoutDenominator(condition_id).call().await?;
        Ok(denom > U256::ZERO)
    }

    /// Redeem a standard (non neg-risk) binary market. Transfers settlement
    /// collateral (pUSD post-V2, USDC.e for legacy positions) to
    /// `position_holder` for the full outstanding balance on both outcomes.
    pub async fn redeem_standard(&self, condition_id: B256) -> anyhow::Result<B256> {
        let ctf = IConditionalTokens::new(self.ctf_address, &self.provider);
        let index_sets = vec![U256::from(1_u64), U256::from(2_u64)];
        let pending = ctf
            .redeemPositions(self.collateral_address, B256::ZERO, condition_id, index_sets)
            .send()
            .await
            .context("CTF.redeemPositions(standard) send failed")?;
        let tx_hash = *pending.tx_hash();
        // Wait for the transaction to land (or revert).
        let _ = pending
            .get_receipt()
            .await
            .context("CTF.redeemPositions(standard) receipt failed")?;
        Ok(tx_hash)
    }

    /// Redeem a neg-risk market. Queries the caller's balance for both YES
    /// and NO outcome position tokens on the underlying CTF (collateral =
    /// WrappedCollateral) and passes them as `[yes_bal, no_bal]`. The
    /// adapter unwraps to the underlying collateral (pUSD post-V2, USDC.e
    /// for legacy positions) and forwards it to `position_holder`.
    pub async fn redeem_neg_risk(&self, condition_id: B256) -> anyhow::Result<B256> {
        let ctf = IConditionalTokens::new(self.ctf_address, &self.provider);

        let yes_collection = ctf
            .getCollectionId(B256::ZERO, condition_id, U256::from(1_u64))
            .call()
            .await
            .context("CTF.getCollectionId(YES) failed")?;
        let no_collection = ctf
            .getCollectionId(B256::ZERO, condition_id, U256::from(2_u64))
            .call()
            .await
            .context("CTF.getCollectionId(NO) failed")?;

        let yes_position_id = ctf
            .getPositionId(self.wcol_address, yes_collection)
            .call()
            .await
            .context("CTF.getPositionId(YES) failed")?;
        let no_position_id = ctf
            .getPositionId(self.wcol_address, no_collection)
            .call()
            .await
            .context("CTF.getPositionId(NO) failed")?;

        let yes_bal = ctf
            .balanceOf(self.position_holder, yes_position_id)
            .call()
            .await
            .context("CTF.balanceOf(YES) failed")?;
        let no_bal = ctf
            .balanceOf(self.position_holder, no_position_id)
            .call()
            .await
            .context("CTF.balanceOf(NO) failed")?;

        if yes_bal.is_zero() && no_bal.is_zero() {
            return Err(anyhow!(
                "neg-risk redeem: both YES and NO balances are zero for condition {}",
                condition_id
            ));
        }

        let adapter = INegRiskAdapter::new(self.neg_risk_adapter, &self.provider);
        let pending = adapter
            .redeemPositions(condition_id, vec![yes_bal, no_bal])
            .send()
            .await
            .context("NegRiskAdapter.redeemPositions send failed")?;
        let tx_hash = *pending.tx_hash();
        let _ = pending
            .get_receipt()
            .await
            .context("NegRiskAdapter.redeemPositions receipt failed")?;
        Ok(tx_hash)
    }

    pub fn position_holder(&self) -> Address {
        self.position_holder
    }
}

/// Parse a `0x`-prefixed hex string into a `B256`. Accepts with or without
/// the `0x` prefix.
pub fn parse_b256(raw: &str) -> anyhow::Result<B256> {
    let trimmed = raw.trim();
    let hex = trimmed.trim_start_matches("0x").trim_start_matches("0X");
    if hex.len() != 64 {
        return Err(anyhow!(
            "expected 32-byte hex (got len={}): '{trimmed}'",
            hex.len()
        ));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(hex, &mut out)
        .with_context(|| format!("invalid hex: '{trimmed}'"))?;
    Ok(B256::from(out))
}

/// Shim around the `hex` crate that alloy re-exports so we don't take another
/// direct dependency.
mod hex {
    pub use alloy::hex::decode_to_slice;
}
