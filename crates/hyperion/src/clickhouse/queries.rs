//! ClickHouse SQL query builders for Hyperion API endpoints.
//! Translates filter/sort parameters to efficient SQL.

use anyhow::{anyhow, Result};

/// Build a SQL query for get_actions.
///
/// `after`/`before` are inclusive `block_num` bounds (matching the old
/// Elasticsearch `range_filters` helper's block-number case - this port
/// drops its secondary "or an ISO timestamp" fallback, see
/// `api::parse_block_bound`), not `global_sequence` bounds: block number is
/// what `GetActionsParams.after`/`.before` have always meant to callers.
#[allow(clippy::too_many_arguments)]
pub fn build_get_actions_query(
    account: Option<&str>,
    filter: Option<&str>,
    skip: usize,
    limit: usize,
    sort_order: &str,
    after_block: Option<u64>,
    before_block: Option<u64>,
    transfer_from: Option<&str>,
    transfer_to: Option<&str>,
    transfer_symbol: Option<&str>,
    transfer_memo: Option<&str>,
) -> Result<String> {
    let mut where_clauses = vec!["1=1".to_string()];

    if let Some(account) = account {
        where_clauses.push(format!("has(notified, '{}')", escape_sql(account)));
    }

    if let Some(filter) = filter {
        let (code_filters, action_filters) = parse_filter(filter)?;
        let mut filter_parts = Vec::new();

        for code in &code_filters {
            filter_parts.push(format!("act_account = '{}'", escape_sql(code)));
        }
        if !filter_parts.is_empty() {
            where_clauses.push(format!("({})", filter_parts.join(" OR ")));
        }

        for action in &action_filters {
            where_clauses.push(format!("act_name = '{}'", escape_sql(action)));
        }
    }

    if let Some(from) = transfer_from {
        where_clauses.push(format!("transfer_from = '{}'", escape_sql(from)));
    }
    if let Some(to) = transfer_to {
        where_clauses.push(format!("transfer_to = '{}'", escape_sql(to)));
    }
    if let Some(symbol) = transfer_symbol {
        where_clauses.push(format!("transfer_symbol = '{}'", escape_sql(symbol)));
    }
    if let Some(memo) = transfer_memo {
        where_clauses.push(format!(
            "positionCaseInsensitive(transfer_memo, '{}') > 0",
            escape_sql(memo)
        ));
    }

    if let Some(after) = after_block {
        where_clauses.push(format!("block_num >= {}", after));
    }
    if let Some(before) = before_block {
        where_clauses.push(format!("block_num <= {}", before));
    }

    let sort_dir = if sort_order == "asc" { "ASC" } else { "DESC" };

    let sql = format!(
        "SELECT * FROM action FINAL WHERE {} ORDER BY global_sequence {} LIMIT {} OFFSET {}",
        where_clauses.join(" AND "),
        sort_dir,
        limit,
        skip
    );

    Ok(sql)
}

/// Build a SQL query for get_actions with total count.
pub fn build_get_actions_with_count_query(
    account: Option<&str>,
    filter: Option<&str>,
    transfer_from: Option<&str>,
    transfer_to: Option<&str>,
    transfer_symbol: Option<&str>,
    transfer_memo: Option<&str>,
) -> Result<String> {
    let mut where_clauses = vec!["1=1".to_string()];

    if let Some(account) = account {
        where_clauses.push(format!("has(notified, '{}')", escape_sql(account)));
    }

    if let Some(filter) = filter {
        let (code_filters, action_filters) = parse_filter(filter)?;
        let mut filter_parts = Vec::new();

        for code in &code_filters {
            filter_parts.push(format!("act_account = '{}'", escape_sql(code)));
        }
        if !filter_parts.is_empty() {
            where_clauses.push(format!("({})", filter_parts.join(" OR ")));
        }

        for action in &action_filters {
            where_clauses.push(format!("act_name = '{}'", escape_sql(action)));
        }
    }

    if let Some(from) = transfer_from {
        where_clauses.push(format!("transfer_from = '{}'", escape_sql(from)));
    }
    if let Some(to) = transfer_to {
        where_clauses.push(format!("transfer_to = '{}'", escape_sql(to)));
    }
    if let Some(symbol) = transfer_symbol {
        where_clauses.push(format!("transfer_symbol = '{}'", escape_sql(symbol)));
    }
    if let Some(memo) = transfer_memo {
        where_clauses.push(format!(
            "positionCaseInsensitive(transfer_memo, '{}') > 0",
            escape_sql(memo)
        ));
    }

    let sql = format!(
        "SELECT count() as total FROM action FINAL WHERE {}",
        where_clauses.join(" AND ")
    );

    Ok(sql)
}

/// Build a SQL query for get_tokens.
pub fn build_get_tokens_query(
    account: Option<&str>,
    code: Option<&str>,
    skip: usize,
    limit: usize,
) -> Result<String> {
    let mut where_clauses = vec!["is_deleted = 0".to_string()];

    if let Some(account) = account {
        where_clauses.push(format!("scope = '{}'", escape_sql(account)));
    }
    if let Some(code) = code {
        where_clauses.push(format!("code = '{}'", escape_sql(code)));
    }

    let sql = format!(
        "SELECT code, scope, symbol, precision, amount FROM token FINAL WHERE {} ORDER BY amount DESC LIMIT {} OFFSET {}",
        where_clauses.join(" AND "),
        limit,
        skip
    );

    Ok(sql)
}

/// Build a SQL query for get_key_accounts.
pub fn build_get_key_accounts_query(public_key: &str, skip: usize, limit: usize) -> Result<String> {
    let sql = format!(
        "SELECT owner, name, parent, last_updated, keys, accounts, threshold FROM perm FINAL WHERE is_deleted = 0 AND has(keys, '{}') ORDER BY owner ASC LIMIT {} OFFSET {}",
        escape_sql(public_key),
        limit,
        skip
    );

    Ok(sql)
}

/// Build a SQL query for get_account.
pub fn build_get_account_query(account: &str) -> Result<String> {
    let sql = format!(
        "SELECT owner, name, parent, last_updated, keys, accounts, threshold FROM perm FINAL WHERE is_deleted = 0 AND owner = '{}' ORDER BY name ASC",
        escape_sql(account)
    );

    Ok(sql)
}

/// Build a SQL query for get_transaction: every action belonging to one
/// transaction, in execution order.
pub fn build_get_transaction_query(trx_id: &str) -> Result<String> {
    Ok(format!(
        "SELECT * FROM action FINAL WHERE trx_id = '{}' ORDER BY global_sequence ASC LIMIT 1000",
        escape_sql(trx_id)
    ))
}

/// Build a SQL query for get_deltas.
#[allow(clippy::too_many_arguments)]
pub fn build_get_deltas_query(
    code: Option<&str>,
    scope: Option<&str>,
    table: Option<&str>,
    payer: Option<&str>,
    present: Option<bool>,
    after_block: Option<u64>,
    before_block: Option<u64>,
    skip: usize,
    limit: usize,
    sort_order: &str,
) -> Result<String> {
    let mut where_clauses = vec!["1=1".to_string()];
    for (field, value) in [
        ("code", code),
        ("scope", scope),
        ("table", table),
        ("payer", payer),
    ] {
        if let Some(value) = value {
            where_clauses.push(format!("{field} = '{}'", escape_sql(value)));
        }
    }
    if let Some(present) = present {
        where_clauses.push(format!("present = {}", if present { 1 } else { 0 }));
    }
    if let Some(after) = after_block {
        where_clauses.push(format!("block_num >= {after}"));
    }
    if let Some(before) = before_block {
        where_clauses.push(format!("block_num <= {before}"));
    }
    let sort_dir = if sort_order == "asc" { "ASC" } else { "DESC" };
    Ok(format!(
        "SELECT * FROM delta FINAL WHERE {} ORDER BY block_num {} LIMIT {} OFFSET {}",
        where_clauses.join(" AND "),
        sort_dir,
        limit,
        skip
    ))
}

/// Build a SQL query for get_deltas' total count.
pub fn build_get_deltas_count_query(
    code: Option<&str>,
    scope: Option<&str>,
    table: Option<&str>,
    payer: Option<&str>,
    present: Option<bool>,
    after_block: Option<u64>,
    before_block: Option<u64>,
) -> Result<String> {
    let mut where_clauses = vec!["1=1".to_string()];
    for (field, value) in [
        ("code", code),
        ("scope", scope),
        ("table", table),
        ("payer", payer),
    ] {
        if let Some(value) = value {
            where_clauses.push(format!("{field} = '{}'", escape_sql(value)));
        }
    }
    if let Some(present) = present {
        where_clauses.push(format!("present = {}", if present { 1 } else { 0 }));
    }
    if let Some(after) = after_block {
        where_clauses.push(format!("block_num >= {after}"));
    }
    if let Some(before) = before_block {
        where_clauses.push(format!("block_num <= {before}"));
    }
    Ok(format!(
        "SELECT count() as total FROM delta FINAL WHERE {}",
        where_clauses.join(" AND ")
    ))
}

/// Build a SQL query for get_abi_snapshot: the latest ABI at or before
/// `block`, or the latest ABI overall when `block` is absent.
pub fn build_get_abi_snapshot_query(contract: &str, block: Option<u64>) -> Result<String> {
    let mut where_clauses = vec![format!("account = '{}'", escape_sql(contract))];
    if let Some(block) = block {
        where_clauses.push(format!("block_num <= {block}"));
    }
    Ok(format!(
        "SELECT block_num, abi FROM abi FINAL WHERE {} ORDER BY block_num DESC LIMIT 1",
        where_clauses.join(" AND ")
    ))
}

/// Build a SQL query for get_created_accounts: `newaccount` actions whose
/// creator is `account`.
pub fn build_get_created_accounts_query(account: &str) -> Result<String> {
    Ok(format!(
        "SELECT newaccount_newact, timestamp, trx_id FROM action FINAL \
         WHERE act_name = 'newaccount' AND newaccount_creator = '{}' \
         ORDER BY global_sequence DESC LIMIT 100",
        escape_sql(account)
    ))
}

/// Build a SQL query for get_creator: the `newaccount` action that created
/// `account`, if any.
pub fn build_get_creator_query(account: &str) -> Result<String> {
    Ok(format!(
        "SELECT newaccount_creator, timestamp, block_num, trx_id FROM action FINAL \
         WHERE act_name = 'newaccount' AND newaccount_newact = '{}' \
         ORDER BY global_sequence ASC LIMIT 1",
        escape_sql(account)
    ))
}

/// Build a SQL query for v1 get_controlled_accounts: permissions whose
/// `accounts` array contains an `actor@permission` entry for `controlling_account`.
pub fn build_get_controlled_accounts_query(controlling_account: &str) -> Result<String> {
    Ok(format!(
        "SELECT DISTINCT owner FROM perm FINAL \
         WHERE is_deleted = 0 AND arrayExists(x -> startsWith(x, '{}@'), accounts) \
         ORDER BY owner ASC LIMIT 1000",
        escape_sql(controlling_account)
    ))
}

/// Parse filter string like "code1:action1,code2:action2" into (codes, actions).
fn parse_filter(filter: &str) -> Result<(Vec<String>, Vec<String>)> {
    let mut codes = Vec::new();
    let mut actions = Vec::new();

    for pair in filter.split(',') {
        let parts: Vec<&str> = pair.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow!("bad filter format: {}", pair));
        }
        let code = parts[0].trim();
        let action = parts[1].trim();

        if code != "*" {
            codes.push(code.to_string());
        }
        if action != "*" {
            actions.push(action.to_string());
        }
    }

    Ok((codes, actions))
}

/// Escape SQL string literals.
fn escape_sql(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter() -> Result<()> {
        let (codes, actions) = parse_filter("eosio.token:transfer,*:approve")?;
        assert_eq!(codes, vec!["eosio.token"]);
        assert_eq!(actions, vec!["transfer", "approve"]);
        Ok(())
    }

    #[test]
    fn test_build_get_actions_query() -> Result<()> {
        let query = build_get_actions_query(
            Some("alice"),
            None,
            0,
            10,
            "desc",
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        assert!(query.contains("has(notified, 'alice')"));
        assert!(query.contains("LIMIT 10"));
        Ok(())
    }
}
