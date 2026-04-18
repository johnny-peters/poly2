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
//!   `ConditionalTokens.redeemPositions(usdc, 0x0, conditionId, [1, 2])`.
//!   The contract auto-redeems the caller's full balance for the YES and NO
//!   outcomes and transfers USDC back.
//!
//! - **NegRisk multi-outcome markets**: call
//!   `NegRiskAdapter.redeemPositions(conditionId, [yes_bal, no_bal])`.
//!   The adapter unwraps `WrappedCollateral` → USDC for us. Because the
//!   adapter transfers the requested amount from `msg.sender`, we must query
//!   the caller's ERC1155 balance for both outcome position IDs first.

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
/// Polymarket ever redeploys.
pub const DEFAULT_CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
pub const DEFAULT_NEG_RISK_ADAPTER: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";
pub const DEFAULT_USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
pub const DEFAULT_RPC_URL: &str = "https://polygon-rpc.com";

/// All the state we need to perform redemptions from a single wallet.
pub struct RedeemClient {
    provider: DynProvider,
    /// Either the EOA (Safe-less mode) or the Polymarket proxy wallet — whoever
    /// actually holds the CTF position tokens. For most Polymarket users this
    /// is the proxy wallet; the EOA only signs.
    position_holder: Address,
    ctf_address: Address,
    neg_risk_adapter: Address,
    usdc_address: Address,
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
        usdc_address: Address,
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
            usdc_address,
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

    /// Redeem a standard (non neg-risk) binary market. Transfers USDC to
    /// `position_holder` for the full outstanding balance on both outcomes.
    pub async fn redeem_standard(&self, condition_id: B256) -> anyhow::Result<B256> {
        let ctf = IConditionalTokens::new(self.ctf_address, &self.provider);
        let index_sets = vec![U256::from(1_u64), U256::from(2_u64)];
        let pending = ctf
            .redeemPositions(self.usdc_address, B256::ZERO, condition_id, index_sets)
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
    /// WrappedCollateral) and passes them as `[yes_bal, no_bal]`.
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
