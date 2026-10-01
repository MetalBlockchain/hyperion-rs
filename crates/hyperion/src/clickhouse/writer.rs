//! ClickHouse batch writer: converts processor documents to TabSeparated format and inserts.

use crate::processor::Doc;
use crate::clickhouse::ClickHouse;
use anyhow::{anyhow, Context, Result};
use std::collections::{HashMap, BTreeMap};

/// Converts a Doc from the processor into a ClickHouse row (TabSeparated fields).
pub fn doc_to_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    match doc.kind {
        "action" => action_row(doc, version),
        "block" => block_row(doc, version),
        "delta" => delta_row(doc, version),
        "abi" => abi_row(doc, version),
        "perm" => perm_row(doc, version),
        "token" => token_row(doc, version),
        _ => Err(anyhow!("unknown document kind: {}", doc.kind)),
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
    let obj = doc.body.as_object().ok_or(anyhow!("action doc must be object"))?;

    Ok(vec![
        obj["block_num"].as_u64().unwrap_or(0).to_string(),
        obj["global_sequence"].as_u64().unwrap_or(0).to_string(),
        obj["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(obj["block_id"].as_str().unwrap_or("")),
        escape_tab_separated(obj["trx_id"].as_str().unwrap_or("")),
        escape_tab_separated(obj["producer"].as_str().unwrap_or("")),
        escape_tab_separated(obj["act"]["account"].as_str().unwrap_or("")),
        escape_tab_separated(obj["act"]["name"].as_str().unwrap_or("")),
        tuple_array_to_string("authorization", obj["act"]["authorization"].as_array().unwrap_or(&vec![])),
        serde_json::to_string(&obj["act"]["data"]).unwrap_or_default(),
        obj["act"]["hex_data"].as_str().map(escape_tab_separated).unwrap_or_default(),
        obj["@transfer"]["from"].as_str().map(|s| s.to_string()).unwrap_or_default(),
        obj["@transfer"]["to"].as_str().map(|s| s.to_string()).unwrap_or_default(),
        obj["@transfer"]["amount"].as_f64().map(|f| f.to_string()).unwrap_or_default(),
        obj["@transfer"]["symbol"].as_str().map(|s| s.to_string()).unwrap_or_default(),
        obj["@transfer"]["memo"].as_str().map(|s| s.to_string()).unwrap_or_default(),
        array_to_string(
            &obj["notified"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        ),
        tuple_array_to_string("receipts", obj["receipts"].as_array().unwrap_or(&vec![])),
        obj["cpu_usage_us"].as_u64().unwrap_or(0).to_string(),
        obj["net_usage_words"].as_u64().unwrap_or(0).to_string(),
        obj["action_ordinal"].as_u64().unwrap_or(0).to_string(),
        obj["creator_action_ordinal"].as_u64().unwrap_or(0).to_string(),
        array_to_string(
            &obj["signatures"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        ),
        version.to_string(),
    ])
}

fn block_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let obj = doc.body.as_object().ok_or(anyhow!("block doc must be object"))?;

    Ok(vec![
        obj["block_num"].as_u64().unwrap_or(0).to_string(),
        obj["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(obj["block_id"].as_str().unwrap_or("")),
        escape_tab_separated(obj["prev_id"].as_str().unwrap_or("")),
        escape_tab_separated(obj["producer"].as_str().unwrap_or("")),
        obj["schedule_version"].as_u64().unwrap_or(0).to_string(),
        obj["trx_count"].as_u64().unwrap_or(0).to_string(),
        obj["cpu_usage_us"].as_u64().unwrap_or(0).to_string(),
        obj["net_usage_words"].as_u64().unwrap_or(0).to_string(),
        version.to_string(),
    ])
}

fn delta_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let obj = doc.body.as_object().ok_or(anyhow!("delta doc must be object"))?;

    Ok(vec![
        obj["block_num"].as_u64().unwrap_or(0).to_string(),
        obj["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(obj["block_id"].as_str().unwrap_or("")),
        escape_tab_separated(obj["code"].as_str().unwrap_or("")),
        escape_tab_separated(obj["scope"].as_str().unwrap_or("")),
        escape_tab_separated(obj["table"].as_str().unwrap_or("")),
        escape_tab_separated(obj["primary_key"].as_str().unwrap_or("")),
        escape_tab_separated(obj["payer"].as_str().unwrap_or("")),
        if obj["present"].as_bool().unwrap_or(false) { "1" } else { "0" }.to_string(),
        serde_json::to_string(&obj["data"]).unwrap_or_default(),
        obj["value_hex"].as_str().map(escape_tab_separated).unwrap_or_default(),
        version.to_string(),
    ])
}

fn abi_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let obj = doc.body.as_object().ok_or(anyhow!("abi doc must be object"))?;

    let actions = obj["actions"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let tables = obj["tables"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(vec![
        obj["block_num"].as_u64().unwrap_or(0).to_string(),
        obj["@timestamp"].as_str().unwrap_or("").to_string(),
        escape_tab_separated(obj["account"].as_str().unwrap_or("")),
        escape_tab_separated(obj["abi"].as_str().unwrap_or("")),
        array_to_string(&actions),
        array_to_string(&tables),
        version.to_string(),
    ])
}

fn perm_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let obj = doc.body.as_object().ok_or(anyhow!("perm doc must be object"))?;

    let keys = obj["keys"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let accounts = obj["accounts"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(vec![
        obj["block_num"].as_u64().unwrap_or(0).to_string(),
        escape_tab_separated(obj["owner"].as_str().unwrap_or("")),
        escape_tab_separated(obj["name"].as_str().unwrap_or("")),
        escape_tab_separated(obj["parent"].as_str().unwrap_or("")),
        obj["last_updated"].as_str().unwrap_or("").to_string(),
        array_to_string(&keys),
        array_to_string(&accounts),
        obj["threshold"].as_u64().unwrap_or(0).to_string(),
        version.to_string(),
    ])
}

fn token_row(doc: &Doc, version: u64) -> Result<Vec<String>> {
    let obj = doc.body.as_object().ok_or(anyhow!("token doc must be object"))?;

    Ok(vec![
        obj["block_num"].as_u64().unwrap_or(0).to_string(),
        escape_tab_separated(obj["code"].as_str().unwrap_or("")),
        escape_tab_separated(obj["scope"].as_str().unwrap_or("")),
        escape_tab_separated(obj["symbol"].as_str().unwrap_or("")),
        obj["precision"].as_u64().unwrap_or(0).to_string(),
        obj["amount"].as_f64().unwrap_or(0.0).to_string(),
        version.to_string(),
    ])
}

/// Batch of rows to be inserted into ClickHouse, organized by table.
pub struct ClickHouseBatch {
    pub rows_by_table: HashMap<&'static str, Vec<Vec<String>>>,
    pub max_block: u32,
    pub count: usize,
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
            .entry(doc.kind)
            .or_insert_with(Vec::new)
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
                let version = (u64::from(checkpoint) << 32) | 0;
                ck.set_checkpoint(checkpoint, version).await?;
                tracing::debug!(checkpoint, "advanced checkpoint");
            }
            continue;
        }
        if closed && jobs.is_empty() {
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
                            (max_block, result)
                        });
                    }
                    None => closed = true,
                }
            }
            result = jobs.join_next(), if !jobs.is_empty() => {
                let (max_block, res) = result.expect("nonempty jobs")
                    .context("clickhouse writer task failed")?;
                ready.insert(completed + ready.len() as u64, (max_block, res));
            }
        }
    }
}
