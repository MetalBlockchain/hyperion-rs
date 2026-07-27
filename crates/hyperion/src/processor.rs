//! Turns raw SHIP block results into Elasticsearch documents, mirroring
//! Hyperion's document model: one flattened doc per unique action (inline
//! notifications collapsed into `notified`/`receipts`), plus block, delta,
//! ABI, permission and token-balance docs.

use crate::abis::AbiCache;
use antelope::{time, AbiDecoder, Asset, Name};
use serde_json::{json, Value};
use ship::{
    AccountRow, ActionTrace, BlockHeader, ContractRow, GetBlocksResult, PermissionRow, TableDelta,
    TransactionTrace,
};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Index,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Doc {
    /// Index kind suffix: `action`, `block`, `delta`, `abi`, `perm`, `token`.
    pub kind: &'static str,
    pub id: Option<String>,
    pub body: Value,
    pub op: Op,
}

impl Doc {
    fn index(kind: &'static str, id: impl Into<Option<String>>, body: Value) -> Self {
        Doc {
            kind,
            id: id.into(),
            body,
            op: Op::Index,
        }
    }

    fn delete(kind: &'static str, id: String) -> Self {
        Doc {
            kind,
            id: Some(id),
            body: Value::Null,
            op: Op::Delete,
        }
    }
}

pub struct Processor {
    /// `(contract, action)` pairs to skip.
    skip_actions: HashSet<(u64, u64)>,
    /// The privileged system account whose `setabi` updates the ABI cache
    /// (`eosio` on Antelope chains, `pulse` on PulseVM).
    system_account: Name,
}

impl Processor {
    pub fn new(skip_actions: &[String], system_account: Name) -> Self {
        let mut skip = HashSet::new();
        for entry in skip_actions {
            if let Some((code, action)) = entry.split_once("::") {
                if let (Ok(code), Ok(action)) = (Name::from_str(code), Name::from_str(action)) {
                    skip.insert((code.0, action.0));
                    continue;
                }
            }
            tracing::warn!(
                entry,
                "ignoring malformed skip_actions entry (want contract::action)"
            );
        }
        Processor {
            skip_actions: skip,
            system_account,
        }
    }

    /// Process one block result into documents. `abis` is consulted for
    /// action/table decoding and updated from on-chain ABI changes.
    pub async fn process_block(
        &self,
        result: &GetBlocksResult,
        abis: &mut AbiCache,
    ) -> anyhow::Result<Vec<Doc>> {
        let Some(this_block) = &result.this_block else {
            return Ok(Vec::new());
        };
        let block_num = this_block.block_num;
        let block_id = this_block.block_id.clone();
        let mut docs = Vec::new();

        let header = match &result.block {
            Some(bytes) => match BlockHeader::decode(bytes) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!(block_num, error = %e, "failed to decode signed_block");
                    None
                }
            },
            None => None,
        };
        let timestamp = header
            .as_ref()
            .map(|h| time::block_timestamp_to_string(h.timestamp_slot));
        let producer = header.as_ref().map(|h| h.producer.to_string());

        if let Some(h) = &header {
            docs.push(Doc::index(
                "block",
                block_num.to_string(),
                json!({
                    "@timestamp": timestamp,
                    "block_num": block_num,
                    "block_id": block_id,
                    "prev_id": h.previous,
                    "producer": h.producer.to_string(),
                    "schedule_version": h.schedule_version,
                    "trx_count": h.trx_count,
                    "cpu_usage_us": h.cpu_usage_us,
                    "net_usage_words": h.net_usage_words,
                }),
            ));
        }

        if let Some(traces) = &result.traces {
            let traces = TransactionTrace::decode_traces(traces)?;
            for trace in &traces {
                self.process_transaction(
                    trace,
                    block_num,
                    &block_id,
                    timestamp.as_deref(),
                    producer.as_deref(),
                    abis,
                    &mut docs,
                )
                .await;
            }
        }

        if let Some(deltas) = &result.deltas {
            let deltas = TableDelta::decode_deltas(deltas)?;
            self.process_deltas(
                &deltas,
                block_num,
                &block_id,
                timestamp.as_deref(),
                abis,
                &mut docs,
            )
            .await;
        }

        Ok(docs)
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_transaction(
        &self,
        trace: &TransactionTrace,
        block_num: u32,
        block_id: &str,
        timestamp: Option<&str>,
        producer: Option<&str>,
        abis: &mut AbiCache,
        docs: &mut Vec<Doc>,
    ) {
        // Only executed transactions produce state-relevant actions.
        if trace.status != 0 {
            return;
        }
        let signatures = trace
            .partial
            .as_ref()
            .map(|p| p.signatures.clone())
            .unwrap_or_default();

        // Collapse notification traces (require_recipient) into their primary
        // action: group by act_digest, primary = trace where receiver ==
        // act.account.
        let mut groups: HashMap<&str, Vec<&ActionTrace>> = HashMap::new();
        let mut order: Vec<&str> = Vec::new();
        for at in &trace.action_traces {
            let Some(receipt) = &at.receipt else { continue };
            let group = groups.entry(receipt.act_digest.as_str()).or_default();
            if group.is_empty() {
                order.push(receipt.act_digest.as_str());
            }
            group.push(at);
        }

        for digest in order {
            let group = &groups[digest];
            let primary = group
                .iter()
                .find(|at| at.receiver == at.act.account)
                .unwrap_or(&group[0]);
            if self
                .skip_actions
                .contains(&(primary.act.account.0, primary.act.name.0))
            {
                continue;
            }
            let receipt = primary
                .receipt
                .as_ref()
                .expect("grouped traces have receipts");

            // Handle system ABI updates inline so contracts deployed and
            // used within the same stream stay decodable.
            if primary.act.account == self.system_account
                && primary.act.name == Name::from_str("setabi").unwrap()
            {
                self.apply_setabi(primary, abis);
            }

            let (data, hex_data) = decode_action_data(primary, abis).await;

            let notified: Vec<String> = group.iter().map(|at| at.receiver.to_string()).collect();
            let receipts: Vec<Value> = group
                .iter()
                .filter_map(|at| at.receipt.as_ref())
                .map(|r| {
                    json!({
                        "receiver": r.receiver.to_string(),
                        "global_sequence": r.global_sequence,
                        "recv_sequence": r.recv_sequence,
                        "auth_sequence": r.auth_sequence.iter().map(|(account, sequence)| json!({
                            "account": account.to_string(),
                            "sequence": sequence,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();

            let mut act = json!({
                "account": primary.act.account.to_string(),
                "name": primary.act.name.to_string(),
                "authorization": primary.act.authorization.iter().map(|a| json!({
                    "actor": a.actor.to_string(),
                    "permission": a.permission.to_string(),
                })).collect::<Vec<_>>(),
            });
            if let Some(data) = data {
                act["data"] = data;
            }
            if let Some(hex) = hex_data {
                act["hex_data"] = json!(hex);
            }

            let mut body = json!({
                "@timestamp": timestamp,
                "global_sequence": receipt.global_sequence,
                "block_num": block_num,
                "block_id": block_id,
                "trx_id": trace.id,
                "producer": producer,
                "act": act,
                "notified": notified,
                "receipts": receipts,
                "action_ordinal": primary.action_ordinal,
                "creator_action_ordinal": primary.creator_action_ordinal,
                "cpu_usage_us": trace.cpu_usage_us,
                "net_usage_words": trace.net_usage_words,
                "code_sequence": receipt.code_sequence,
                "abi_sequence": receipt.abi_sequence,
                "act_digest": receipt.act_digest,
            });
            // Signatures only on root actions, like Hyperion, to avoid
            // duplicating them across every inline action.
            if primary.creator_action_ordinal == 0 && !signatures.is_empty() {
                body["signatures"] = json!(signatures);
            }
            docs.push(Doc::index(
                "action",
                receipt.global_sequence.to_string(),
                body,
            ));
        }
    }

    fn apply_setabi(&self, trace: &ActionTrace, abis: &mut AbiCache) {
        // setabi data: {account: name, abi: bytes} — fixed layout, decodable
        // without the system ABI.
        let mut r = antelope::ByteReader::new(&trace.act.data);
        let parsed = (|| -> antelope::Result<(Name, Vec<u8>)> {
            let account = r.read_name()?;
            let abi = r.read_bytes()?.to_vec();
            Ok((account, abi))
        })();
        if let Ok((account, abi)) = parsed {
            abis.update(account, &abi);
        }
    }

    async fn process_deltas(
        &self,
        deltas: &[TableDelta],
        block_num: u32,
        block_id: &str,
        timestamp: Option<&str>,
        abis: &mut AbiCache,
        docs: &mut Vec<Doc>,
    ) {
        let mut ordinal = 0u32;
        for delta in deltas {
            match delta.name.as_str() {
                "account" => {
                    for row in &delta.rows {
                        let Ok(account) = AccountRow::decode(&row.data) else {
                            continue;
                        };
                        if !row.present || account.abi.is_empty() {
                            continue;
                        }
                        let parsed = abis.update(account.name, &account.abi);
                        let Some(abi) = parsed else { continue };
                        docs.push(Doc::index(
                            "abi",
                            format!("{block_num}-{}", account.name),
                            json!({
                                "@timestamp": timestamp,
                                "block_num": block_num,
                                "account": account.name.to_string(),
                                "abi": serde_json::to_string(abi.as_ref()).unwrap_or_default(),
                                "actions": abi.actions.iter().map(|a| a.name.to_string()).collect::<Vec<_>>(),
                                "tables": abi.tables.iter().map(|t| t.name.to_string()).collect::<Vec<_>>(),
                            }),
                        ));
                    }
                }
                "permission" => {
                    for row in &delta.rows {
                        let Ok(perm) = PermissionRow::decode(&row.data) else {
                            continue;
                        };
                        let id = format!("{}-{}", perm.owner, perm.name);
                        if row.present {
                            docs.push(Doc::index(
                                "perm",
                                id,
                                json!({
                                    "block_num": block_num,
                                    "owner": perm.owner.to_string(),
                                    "name": perm.name.to_string(),
                                    "parent": perm.parent.to_string(),
                                    "last_updated": time::time_point_to_string(perm.last_updated),
                                    "threshold": perm.threshold,
                                    "keys": perm.keys,
                                    "accounts": perm.accounts,
                                }),
                            ));
                        } else {
                            docs.push(Doc::delete("perm", id));
                        }
                    }
                }
                "contract_row" => {
                    for row in &delta.rows {
                        let Ok(cr) = ContractRow::decode(&row.data) else {
                            continue;
                        };
                        self.process_contract_row(
                            &cr,
                            row.present,
                            block_num,
                            block_id,
                            timestamp,
                            ordinal,
                            abis,
                            docs,
                        )
                        .await;
                        ordinal += 1;
                    }
                }
                _ => {}
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_contract_row(
        &self,
        row: &ContractRow,
        present: bool,
        block_num: u32,
        block_id: &str,
        timestamp: Option<&str>,
        ordinal: u32,
        abis: &mut AbiCache,
        docs: &mut Vec<Doc>,
    ) {
        let decoded = match abis.get_or_fetch(row.code).await {
            Some(abi) => AbiDecoder::new(&abi)
                .decode_table_row(row.table, &row.value)
                .ok(),
            None => None,
        };

        let mut body = json!({
            "@timestamp": timestamp,
            "block_num": block_num,
            "block_id": block_id,
            "code": row.code.to_string(),
            "scope": row.scope.to_string(),
            "table": row.table.to_string(),
            "primary_key": row.primary_key.to_string(),
            "payer": row.payer.to_string(),
            "present": present,
        });
        match &decoded {
            Some(data) => body["data"] = data.clone(),
            None => body["value_hex"] = json!(hex::encode(&row.value)),
        }
        docs.push(Doc::index("delta", format!("{block_num}-{ordinal}"), body));

        // Token balances: the conventional `accounts` table with an asset
        // `balance` field, as implemented by eosio.token forks.
        if row.table == Name::from_str("accounts").unwrap() {
            if let Some(balance) = decoded
                .as_ref()
                .and_then(|d| d.get("balance"))
                .and_then(|b| b.as_str())
                .and_then(parse_asset)
            {
                let id = format!("{}-{}-{}", row.code, row.scope, balance.symbol.code());
                if present {
                    docs.push(Doc::index(
                        "token",
                        id,
                        json!({
                            "block_num": block_num,
                            "code": row.code.to_string(),
                            "scope": row.scope.to_string(),
                            "symbol": balance.symbol.code().to_string(),
                            "precision": balance.symbol.precision(),
                            "amount": balance.to_f64(),
                        }),
                    ));
                } else {
                    docs.push(Doc::delete("token", id));
                }
            }
        }
    }
}

async fn decode_action_data(
    trace: &ActionTrace,
    abis: &mut AbiCache,
) -> (Option<Value>, Option<String>) {
    if trace.act.data.is_empty() {
        return (Some(json!({})), None);
    }
    if let Some(abi) = abis.get_or_fetch(trace.act.account).await {
        if let Ok(value) = AbiDecoder::new(&abi).decode_action(trace.act.name, &trace.act.data) {
            return (Some(value), None);
        }
    }
    (None, Some(hex::encode(&trace.act.data)))
}

/// Parse an asset string like `1.0000 EOS`.
fn parse_asset(s: &str) -> Option<Asset> {
    let (amount_str, code) = s.split_once(' ')?;
    let symbol_code: antelope::SymbolCode = code.parse().ok()?;
    let (int_part, frac_part) = match amount_str.split_once('.') {
        Some((i, f)) => (i, f),
        None => (amount_str, ""),
    };
    let precision = frac_part.len() as u8;
    let negative = int_part.starts_with('-');
    let int: i64 = int_part.parse().ok()?;
    let frac: i64 = if frac_part.is_empty() {
        0
    } else {
        frac_part.parse().ok()?
    };
    let scale = 10i64.checked_pow(precision as u32)?;
    let magnitude = int.abs().checked_mul(scale)?.checked_add(frac)?;
    let amount = if negative { -magnitude } else { magnitude };
    let symbol = antelope::Symbol((symbol_code.0 << 8) | precision as u64);
    Some(Asset::new(amount, symbol))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_assets() {
        let a = parse_asset("1.0000 EOS").unwrap();
        assert_eq!(a.amount, 10000);
        assert_eq!(a.symbol.precision(), 4);
        assert_eq!(a.to_string(), "1.0000 EOS");

        let a = parse_asset("-0.5 WAX").unwrap();
        assert_eq!(a.amount, -5);
        assert_eq!(a.symbol.precision(), 1);

        let a = parse_asset("42 SYS").unwrap();
        assert_eq!(a.amount, 42);
        assert_eq!(a.symbol.precision(), 0);

        assert!(parse_asset("garbage").is_none());
        assert!(parse_asset("1.0 lowercase").is_none());
    }
}
