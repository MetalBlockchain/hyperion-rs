//! ClickHouse batch writer: converts processor documents to TabSeparated format and inserts.

use crate::clickhouse::ClickHouse;
use crate::processor::{Doc, Op};
use anyhow::{anyhow, Context, Result};
use std::collections::{BTreeMap, HashMap};

/// Converts a Doc from the processor into a ClickHouse row (TabSeparated fields).
///
/// `perm` and `token` are current-state snapshot tables (see the comment on
/// the `perm` table in `schema.rs`), so a delete there is written as a
/// tombstone row sharing the same sort key with `is_deleted = 1`, which
/// ReplacingMergeTree then resolves in place of the live row. `action`,
/// `block`, `delta`, and `abi` are append-only history: the processor never
/// deletes from them (see `processor::Doc::delete` call sites).
pub fn doc_to_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    match (doc.kind, doc.op) {
        ("action", Op::Index) => action_row(doc, version),
        ("block", Op::Index) => block_row(doc, version),
        ("delta", Op::Index) => delta_row(doc, version),
        ("abi", Op::Index) => abi_row(doc, version),
        ("perm", Op::Index) => perm_row(doc, version),
        ("perm", Op::Delete) => perm_tombstone_row(doc, version),
        ("token", Op::Index) => token_row(doc, version),
        ("token", Op::Delete) => token_tombstone_row(doc, version),
        (kind, Op::Delete) => Err(anyhow!("document kind {kind} does not support delete")),
        (kind, Op::Index) => Err(anyhow!("unknown document kind: {kind}")),
    }
}

fn escape_tab_separated(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn array_to_string(arr: &[String]) -> String {
    format!(
        "[{}]",
        arr.iter()
            .map(|s| format!("'{}'", escape_tab_separated(s)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn tuple_array_to_string(name: &str, items: &[serde_json::Value]) -> String {
    format!(
        "[{}]",
        items
            .iter()
            .filter_map(|item| {
                // `item` comes straight from `processor.rs`'s own
                // construction, where both fields of each tuple are always
                // set together (never one without the other), so indexing
                // this per-item map directly is safe in practice.
                let obj = item.as_object()?;
                let fields: Vec<String> = match name {
                    "authorization" => vec![
                        format!("'{}'", escape_tab_separated(obj["actor"].as_str()?)),
                        format!("'{}'", escape_tab_separated(obj["permission"].as_str()?)),
                    ],
                    "receipts" => vec![
                        format!("'{}'", escape_tab_separated(obj["receiver"].as_str()?)),
                        obj["global_sequence"].as_u64()?.to_string(),
                    ],
                    _ => return None,
                };
                Some(format!("({})", fields.join(",")))
            })
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn action_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    // `doc.body.as_object()` only confirms the top level is an object; the
    // JSON *values* module's `Index` impl for `Value` is safe (a missing key
    // yields `Value::Null`), but `serde_json::Map`'s own `Index` impl is NOT
    // - `map["missing"]` panics ("no entry found for key"). `@transfer`,
    // `@newaccount`, and `signatures` are all conditionally absent (see
    // `processor.rs`'s body construction: `@transfer`/`@newaccount` are only
    // set for matching actions, `signatures` only for root actions), so
    // every access below indexes `doc.body` (a `Value`, safe) rather than
    // `obj` (a `Map`, not safe) for any key that isn't guaranteed present.
    let _ = doc
        .body
        .as_object()
        .ok_or(anyhow!("action doc must be object"))?;

    Ok(vec![
        doc.body["block_num"].as_u64().unwrap_or(0).to_string(),
        doc.body["global_sequence"]
            .as_u64()
            .unwrap_or(0)
            .to_string(),
        doc.body["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(doc.body["block_id"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["trx_id"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["producer"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["act"]["account"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["act"]["name"].as_str().unwrap_or("")),
        tuple_array_to_string(
            "authorization",
            doc.body["act"]["authorization"]
                .as_array()
                .unwrap_or(&vec![]),
        ),
        serde_json::to_string(&doc.body["act"]["data"]).unwrap_or_default(),
        doc.body["act"]["hex_data"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        doc.body["@transfer"]["from"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        doc.body["@transfer"]["to"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        doc.body["@transfer"]["amount"]
            .as_f64()
            .map(|f| f.to_string())
            .unwrap_or_default(),
        doc.body["@transfer"]["symbol"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        doc.body["@transfer"]["memo"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        doc.body["@newaccount"]["creator"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        doc.body["@newaccount"]["newact"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        array_to_string(
            &doc.body["notified"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        ),
        tuple_array_to_string(
            "receipts",
            doc.body["receipts"].as_array().unwrap_or(&vec![]),
        ),
        doc.body["cpu_usage_us"].as_u64().unwrap_or(0).to_string(),
        doc.body["net_usage_words"]
            .as_u64()
            .unwrap_or(0)
            .to_string(),
        doc.body["action_ordinal"].as_u64().unwrap_or(0).to_string(),
        doc.body["creator_action_ordinal"]
            .as_u64()
            .unwrap_or(0)
            .to_string(),
        array_to_string(
            &doc.body["signatures"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        ),
        version.to_string(),
    ])
}

fn block_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let _ = doc
        .body
        .as_object()
        .ok_or(anyhow!("block doc must be object"))?;

    Ok(vec![
        doc.body["block_num"].as_u64().unwrap_or(0).to_string(),
        doc.body["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(doc.body["block_id"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["prev_id"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["producer"].as_str().unwrap_or("")),
        doc.body["schedule_version"]
            .as_u64()
            .unwrap_or(0)
            .to_string(),
        doc.body["trx_count"].as_u64().unwrap_or(0).to_string(),
        doc.body["cpu_usage_us"].as_u64().unwrap_or(0).to_string(),
        doc.body["net_usage_words"]
            .as_u64()
            .unwrap_or(0)
            .to_string(),
        version.to_string(),
    ])
}

fn delta_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let _ = doc
        .body
        .as_object()
        .ok_or(anyhow!("delta doc must be object"))?;

    Ok(vec![
        doc.body["block_num"].as_u64().unwrap_or(0).to_string(),
        doc.body["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(doc.body["block_id"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["code"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["scope"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["table"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["primary_key"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["payer"].as_str().unwrap_or("")),
        if doc.body["present"].as_bool().unwrap_or(false) {
            "1"
        } else {
            "0"
        }
        .to_string(),
        serde_json::to_string(&doc.body["data"]).unwrap_or_default(),
        doc.body["value_hex"]
            .as_str()
            .map(escape_tab_separated)
            .unwrap_or_default(),
        version.to_string(),
    ])
}

fn abi_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let _ = doc
        .body
        .as_object()
        .ok_or(anyhow!("abi doc must be object"))?;

    let actions = doc.body["actions"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let tables = doc.body["tables"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(vec![
        doc.body["block_num"].as_u64().unwrap_or(0).to_string(),
        doc.body["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(doc.body["account"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["abi"].as_str().unwrap_or("")),
        array_to_string(&actions),
        array_to_string(&tables),
        version.to_string(),
    ])
}

fn perm_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let _ = doc
        .body
        .as_object()
        .ok_or(anyhow!("perm doc must be object"))?;

    let keys = doc.body["keys"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let accounts = doc.body["accounts"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(vec![
        doc.body["block_num"].as_u64().unwrap_or(0).to_string(),
        escape_tab_separated(doc.body["owner"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["name"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["parent"].as_str().unwrap_or("")),
        doc.body["last_updated"].as_str().unwrap_or("").to_string(),
        array_to_string(&keys),
        array_to_string(&accounts),
        doc.body["threshold"].as_u64().unwrap_or(0).to_string(),
        version.to_string(),
        "0".to_string(),
    ])
}

/// `doc.id` is `"{owner}-{name}"` (see `processor::Doc::delete("perm", id)`'s
/// call site); EOSIO/Antelope names never contain `-`, so splitting on the
/// first one is unambiguous. Only the sort-key fields are meaningful here -
/// ReplacingMergeTree discards the rest of this row's columns once it
/// resolves `is_deleted`, but the API layer must still filter it explicitly
/// (`WHERE is_deleted = 0`) since `FINAL` alone only picks the latest
/// version, not "the latest version that isn't a tombstone". `last_updated`
/// is non-null in ClickHouse, so tombstones use the epoch as a placeholder;
/// the version column alone controls replacement order.
fn perm_tombstone_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let id = doc
        .id
        .as_deref()
        .ok_or(anyhow!("perm delete requires an id"))?;
    let (owner, name) = id
        .split_once('-')
        .ok_or_else(|| anyhow!("malformed perm delete id: {id}"))?;
    Ok(vec![
        "0".to_string(),
        escape_tab_separated(owner),
        escape_tab_separated(name),
        String::new(),
        "1970-01-01 00:00:00.000".to_string(),
        array_to_string(&[]),
        array_to_string(&[]),
        "0".to_string(),
        version.to_string(),
        "1".to_string(),
    ])
}

fn token_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let _ = doc
        .body
        .as_object()
        .ok_or(anyhow!("token doc must be object"))?;

    Ok(vec![
        doc.body["block_num"].as_u64().unwrap_or(0).to_string(),
        escape_tab_separated(doc.body["code"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["scope"].as_str().unwrap_or("")),
        escape_tab_separated(doc.body["symbol"].as_str().unwrap_or("")),
        doc.body["precision"].as_u64().unwrap_or(0).to_string(),
        doc.body["amount"].as_f64().unwrap_or(0.0).to_string(),
        version.to_string(),
        "0".to_string(),
    ])
}

/// `doc.id` is `"{code}-{scope}-{symbol}"` (see
/// `processor::Doc::delete("token", id)`'s call site). Token symbol codes are
/// uppercase `A-Z` only and account names never contain `-`, so splitting on
/// `-` from the left twice is unambiguous. See `perm_tombstone_row` for why
/// the API layer still needs its own `is_deleted` filter.
fn token_tombstone_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let id = doc
        .id
        .as_deref()
        .ok_or(anyhow!("token delete requires an id"))?;
    let mut parts = id.splitn(3, '-');
    let (code, scope, symbol) = match (parts.next(), parts.next(), parts.next()) {
        (Some(code), Some(scope), Some(symbol)) => (code, scope, symbol),
        _ => return Err(anyhow!("malformed token delete id: {id}")),
    };
    Ok(vec![
        "0".to_string(),
        escape_tab_separated(code),
        escape_tab_separated(scope),
        escape_tab_separated(symbol),
        "0".to_string(),
        "0".to_string(),
        version.to_string(),
        "1".to_string(),
    ])
}

/// Batch of rows to be inserted into ClickHouse, organized by table.
pub struct ClickHouseBatch {
    pub rows_by_table: HashMap<String, Vec<Vec<String>>>,
    pub max_block: u32,
    pub count: usize,
}

impl Default for ClickHouseBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl ClickHouseBatch {
    pub fn new() -> Self {
        ClickHouseBatch {
            rows_by_table: HashMap::new(),
            max_block: 0,
            count: 0,
        }
    }

    pub fn push(&mut self, doc: &Doc, version: u64) -> Result<()> {
        let row = doc_to_row(doc, version)?;
        let block_num = (version >> 32) as u32;

        self.rows_by_table
            .entry(doc.kind.to_string())
            .or_default()
            .push(row);

        self.max_block = self.max_block.max(block_num);
        self.count += 1;
        Ok(())
    }

    /// Convert batch to TabSeparated format, ready for insertion.
    pub fn to_tab_separated(&self, table: &str) -> String {
        let rows = match self.rows_by_table.get(table) {
            Some(rows) => rows,
            None => return String::new(),
        };

        rows.iter()
            .map(|fields| fields.join("\t"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Write batches to ClickHouse concurrently, maintaining checkpoint in order.
pub async fn write_batches(
    ck: &ClickHouse,
    mut rx: tokio::sync::mpsc::Receiver<ClickHouseBatch>,
    concurrency: usize,
) -> Result<()> {
    anyhow::ensure!(concurrency > 0, "writer_concurrency must be at least 1");

    let mut jobs = tokio::task::JoinSet::new();
    let mut ready: BTreeMap<u64, (u32, Result<()>)> = BTreeMap::new();
    let mut submitted = 0u64;
    let mut completed = 0u64;
    let mut checkpoint = 0u32;
    let mut closed = false;

    loop {
        if let Some((max_block, result)) = ready.remove(&completed) {
            result?;
            completed += 1;
            if max_block > checkpoint {
                checkpoint = max_block;
                let version = (checkpoint as u64) << 32;
                ck.set_checkpoint(checkpoint, version).await?;
                tracing::debug!(checkpoint, "advanced checkpoint");
            }
            continue;
        }
        if closed && jobs.is_empty() {
            // Process any remaining queued results before exit (ensure no lost checkpoints)
            while let Some((max_block, result)) = ready.remove(&completed) {
                result?;
                completed += 1;
                if max_block > checkpoint {
                    checkpoint = max_block;
                    let version = (checkpoint as u64) << 32;
                    ck.set_checkpoint(checkpoint, version).await?;
                    tracing::debug!(checkpoint, "advanced final checkpoint");
                }
            }
            return Ok(());
        }

        tokio::select! {
            batch = rx.recv(), if !closed && jobs.len() + ready.len() < concurrency => {
                match batch {
                    Some(batch) => {
                        let ordinal = submitted;
                        submitted += 1;
                        let max_block = batch.max_block;
                        let count = batch.count;
                        let ck = ck.clone();

                        jobs.spawn(async move {
                            let start = std::time::Instant::now();

                            // Insert each table in the batch
                            let mut result = Ok(());
                            for table in &["action", "block", "delta", "abi", "perm", "token"] {
                                let data = batch.to_tab_separated(table);
                                if !data.is_empty() {
                                    if let Err(e) = ck.insert_tab_separated(table, data).await {
                                        result = Err(e);
                                        break;
                                    }
                                }
                            }

                            if result.is_ok() {
                                tracing::debug!(
                                    count,
                                    tables = batch.rows_by_table.len(),
                                    elapsed_ms = start.elapsed().as_millis(),
                                    "batch inserted"
                                );
                            }
                            (ordinal, max_block, result)
                        });
                    }
                    None => closed = true,
                }
            }
            result = jobs.join_next(), if !jobs.is_empty() => {
                let (ordinal, max_block, res) = result.expect("nonempty jobs")
                    .context("clickhouse writer task failed")?;
                ready.insert(ordinal, (max_block, res));
            }
        }
    }
}
