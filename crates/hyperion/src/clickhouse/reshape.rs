//! Reconstructs the nested JSON document shape the API handlers expect
//! (`act.account`, `@transfer.memo`, ...) from a flat ClickHouse row.
//!
//! `writer::action_row` flattens an action `Doc` into the columns declared in
//! `schema::create_tables_sql`; this is the inverse, applied to the rows
//! `ClickHouse::query_rows` returns from a `SELECT * FROM action ...`. It
//! depends on ClickHouse's JSON/JSONEachRow format rendering a named
//! `Tuple(field String, ...)` column as a JSON object keyed by field name
//! (documented ClickHouse behavior) rather than a positional array - if a
//! ClickHouse version ever changes that, `act_authorization`/`receipts`
//! below need to switch from `.get("actor")` to a positional read.
//!
//! `block`, `delta`, `abi`, `perm`, and `token` rows need no such reshaping:
//! their columns are already flat and match the field names the API expects
//! (compare `schema::create_tables_sql` to the original `processor.rs` doc
//! bodies for those kinds).

use serde_json::{json, Value};

/// `block_num`/`global_sequence`/etc. come back from ClickHouse as JSON
/// numbers already; this only needs to handle the handful of fields whose
/// on-disk representation differs from the nested shape callers expect.
pub fn action_doc(row: &Value) -> Value {
    let act_data: Value = row["act_data"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);

    let mut act = json!({
        "account": row["act_account"],
        "name": row["act_name"],
        "authorization": row["act_authorization"],
        "data": act_data,
    });
    if let Some(hex) = row["act_hex_data"].as_str() {
        act["hex_data"] = json!(hex);
    }

    let mut doc = json!({
        "@timestamp": row["timestamp"],
        "block_num": row["block_num"],
        "global_sequence": row["global_sequence"],
        "block_id": row["block_id"],
        "trx_id": row["trx_id"],
        "producer": row["producer"],
        "act": act,
        "notified": row["notified"],
        "receipts": row["receipts"],
        "action_ordinal": row["action_ordinal"],
        "creator_action_ordinal": row["creator_action_ordinal"],
        "cpu_usage_us": row["cpu_usage_us"],
        "net_usage_words": row["net_usage_words"],
        "signatures": row["signatures"],
    });

    if row["transfer_from"].is_string() {
        doc["@transfer"] = json!({
            "from": row["transfer_from"],
            "to": row["transfer_to"],
            "amount": row["transfer_amount"],
            "symbol": row["transfer_symbol"],
            "memo": row["transfer_memo"],
        });
    }
    if row["newaccount_newact"].is_string() {
        doc["@newaccount"] = json!({
            "creator": row["newaccount_creator"],
            "newact": row["newaccount_newact"],
        });
    }

    doc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reshapes_a_flat_row_back_into_the_nested_action_doc_shape() {
        let row = json!({
            "block_num": 10,
            "global_sequence": 99,
            "timestamp": "2024-01-01 00:00:00",
            "block_id": "ab",
            "trx_id": "cd",
            "producer": "pulse",
            "act_account": "eosio.token",
            "act_name": "transfer",
            "act_authorization": [{"actor": "alice", "permission": "active"}],
            "act_data": "{\"from\":\"alice\",\"to\":\"bob\"}",
            "act_hex_data": null,
            "transfer_from": "alice",
            "transfer_to": "bob",
            "transfer_amount": 1.5,
            "transfer_symbol": "EOS",
            "transfer_memo": "hi",
            "newaccount_creator": null,
            "newaccount_newact": null,
            "notified": ["alice", "bob"],
            "receipts": [{"receiver": "alice", "global_sequence": 99}],
            "cpu_usage_us": 100,
            "net_usage_words": 10,
            "action_ordinal": 1,
            "creator_action_ordinal": 0,
            "signatures": ["SIG_K1_x"],
        });
        let doc = action_doc(&row);
        assert_eq!(doc["act"]["account"], "eosio.token");
        assert_eq!(doc["act"]["authorization"][0]["actor"], "alice");
        assert_eq!(doc["act"]["data"]["from"], "alice");
        assert_eq!(doc["@transfer"]["memo"], "hi");
        assert!(doc.get("@newaccount").is_none());
    }

    #[test]
    fn includes_newaccount_only_when_present() {
        let mut row = json!({
            "block_num": 1, "global_sequence": 1, "timestamp": "2024-01-01 00:00:00",
            "block_id": "", "trx_id": "", "producer": "", "act_account": "eosio",
            "act_name": "newaccount", "act_authorization": [], "act_data": "{}",
            "act_hex_data": null, "transfer_from": null, "transfer_to": null,
            "transfer_amount": null, "transfer_symbol": null, "transfer_memo": null,
            "newaccount_creator": "alice", "newaccount_newact": "bob",
            "notified": [], "receipts": [], "cpu_usage_us": 0, "net_usage_words": 0,
            "action_ordinal": 1, "creator_action_ordinal": 0, "signatures": [],
        });
        let doc = action_doc(&row);
        assert_eq!(doc["@newaccount"]["creator"], "alice");
        assert_eq!(doc["@newaccount"]["newact"], "bob");
        assert!(doc.get("@transfer").is_none());

        row["newaccount_creator"] = Value::Null;
        row["newaccount_newact"] = Value::Null;
        assert!(action_doc(&row).get("@newaccount").is_none());
    }
}
