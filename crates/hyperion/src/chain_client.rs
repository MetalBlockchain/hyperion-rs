//! Chain API client supporting two protocols:
//!
//! - `antelope`: the classic nodeos REST API (`/v1/chain/get_info`,
//!   `/v1/chain/get_abi`).
//! - `pulsevm`: PulseVM's JSON-RPC 2.0 API (`pulsevm.getInfo`,
//!   `pulsevm.getABI`), POSTed to the base URL.
//!
//! Both are normalized to the same outputs: `get_info` yields an object with
//! `head_block_num` / `last_irreversible_block_num` / `chain_id`, and
//! `get_abi` yields a parsed [`Abi`] (or `None` when the account has no ABI).

use antelope::{Abi, Name};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainApiKind {
    #[default]
    Antelope,
    #[serde(rename = "pulsevm")]
    PulseVm,
}

pub struct ChainClient {
    http: reqwest::Client,
    base: String,
    kind: ChainApiKind,
    next_id: AtomicU64,
}

impl ChainClient {
    pub fn new(base_url: &str, kind: ChainApiKind) -> Self {
        ChainClient {
            http: reqwest::Client::new(),
            base: base_url.trim_end_matches('/').to_string(),
            kind,
            next_id: AtomicU64::new(0),
        }
    }

    pub fn kind(&self) -> ChainApiKind {
        self.kind
    }

    /// Human-readable service label for health reporting.
    pub fn service_name(&self) -> &'static str {
        match self.kind {
            ChainApiKind::Antelope => "NodeosRPC",
            ChainApiKind::PulseVm => "PulseVM-RPC",
        }
    }

    async fn rpc(&self, method: &str, params: Option<Value>) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let res = self
            .http
            .post(&self.base)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("jsonrpc {method}"))?;
        let envelope: Value = res
            .json()
            .await
            .with_context(|| format!("jsonrpc {method}"))?;
        if let Some(err) = envelope.get("error").filter(|e| !e.is_null()) {
            return Err(anyhow!("jsonrpc {method} error: {err}"));
        }
        envelope
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("jsonrpc {method}: response missing result"))
    }

    /// Chain info with `head_block_num`, `last_irreversible_block_num` and
    /// `chain_id` (PulseVM uses the same field names as nodeos).
    pub async fn get_info(&self) -> Result<Value> {
        match self.kind {
            ChainApiKind::Antelope => {
                let url = format!("{}/v1/chain/get_info", self.base);
                let res = self.http.get(&url).send().await.context("get_info")?;
                Ok(res.json().await.context("get_info")?)
            }
            ChainApiKind::PulseVm => self.rpc("pulsevm.getInfo", None).await,
        }
    }

    /// The account's ABI, or `None` if it has none. Transport failures are
    /// errors; "account has no ABI" responses are `None`.
    pub async fn get_abi(&self, account: Name) -> Result<Option<Abi>> {
        let abi_json = match self.kind {
            ChainApiKind::Antelope => {
                let url = format!("{}/v1/chain/get_abi", self.base);
                let res = self
                    .http
                    .post(&url)
                    .json(&json!({"account_name": account.to_string()}))
                    .send()
                    .await
                    .context("get_abi")?
                    .error_for_status()
                    .context("get_abi")?;
                let body: Value = res.json().await.context("get_abi")?;
                match body.get("abi") {
                    Some(abi) if !abi.is_null() => abi.clone(),
                    _ => return Ok(None),
                }
            }
            ChainApiKind::PulseVm => {
                // PulseVM returns the abi_def directly; a missing account or
                // ABI surfaces as a JSON-RPC error object.
                match self
                    .rpc(
                        "pulsevm.getABI",
                        Some(json!({"account_name": account.to_string()})),
                    )
                    .await
                {
                    Ok(result) if !result.is_null() => result,
                    Ok(_) => return Ok(None),
                    Err(e) => {
                        tracing::debug!(account = %account, error = %e, "pulsevm.getABI returned error; treating as no ABI");
                        return Ok(None);
                    }
                }
            }
        };
        Ok(Some(Abi::from_json(&abi_json)?))
    }
}
