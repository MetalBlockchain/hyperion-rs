//! Minimal ClickHouse HTTP client over reqwest.
//! Uses TabSeparated format for fast bulk inserts.

use anyhow::{anyhow, Context, Result};
use serde_json::Value;

#[derive(Clone)]
pub struct ClickHouse {
    http: reqwest::Client,
    base: String,
    user: Option<String>,
    pass: Option<String>,
}

impl ClickHouse {
    pub fn new(url: impl Into<String>, user: Option<String>, pass: Option<String>) -> Self {
        ClickHouse {
            http: reqwest::Client::new(),
            base: url.into().trim_end_matches('/').to_string(),
            user,
            pass,
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let req = self.http.request(method, format!("{}{path}", self.base));
        match (&self.user, &self.pass) {
            (Some(user), Some(pass)) => req.basic_auth(user, Some(pass)),
            _ => req,
        }
    }

    async fn _json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
    ) -> Result<Value> {
        let mut req = self.request(method.clone(), path);
        if let Some(body) = body {
            req = req.header("Content-Type", "text/plain").body(body);
        }
        let res = req
            .send()
            .await
            .with_context(|| format!("clickhouse {method} {path}"))?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!(
                "clickhouse {method} {path} failed ({status}): {text}"
            ));
        }
        // ClickHouse returns plain text responses for most queries
        Ok(Value::String(text))
    }

    pub async fn ping(&self) -> Result<()> {
        let req = self.request(reqwest::Method::GET, "/ping");
        let res = req.send().await.context("clickhouse ping")?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!(
                "clickhouse ping failed with status {}",
                res.status()
            ))
        }
    }

    /// Execute a `SELECT` and parse each result row as a JSON object.
    ///
    /// Appends `FORMAT JSONEachRow` to `sql` (the query builders in
    /// `clickhouse::queries` never include a `FORMAT` clause themselves) and
    /// parses the newline-delimited JSON response. Column names come from the
    /// query's own `SELECT` list, so a bare `SELECT *` yields the table's
    /// declared column names (see `schema::create_tables_sql`).
    pub async fn query_rows(&self, sql: &str) -> Result<Vec<Value>> {
        let text = self.query(&format!("{sql} FORMAT JSONEachRow")).await?;
        text.lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .with_context(|| format!("invalid JSONEachRow line: {line}"))
            })
            .collect()
    }

    /// Execute a raw SQL query. Returns the response as text.
    pub async fn query(&self, sql: &str) -> Result<String> {
        let req = self
            .request(reqwest::Method::GET, "/")
            .query(&[("query", sql)])
            .header("Accept", "text/plain");
        let res = req.send().await.context("clickhouse query")?;
        let status = res.status();
        let text = res.text().await.context("clickhouse response body")?;
        if !status.is_success() {
            return Err(anyhow!("clickhouse query failed ({status}): {text}"));
        }
        Ok(text)
    }

    /// Execute SQL that mutates state (DDL, checkpoint/progress inserts,
    /// DROP) and returns no result rows.
    ///
    /// Must POST, not GET: ClickHouse's HTTP interface treats every GET as
    /// implicitly read-only and rejects anything else with `Cannot execute
    /// query in readonly mode` - confirmed against a real server. `query()`
    /// (GET, for `SELECT`s) and `execute()` (POST, for everything else) look
    /// similar but are not interchangeable.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        let req = self
            .request(reqwest::Method::POST, "/")
            .header("Content-Type", "text/plain")
            .body(sql.to_string());
        let res = req.send().await.context("clickhouse execute")?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow!("clickhouse execute failed ({status}): {text}"));
        }
        Ok(())
    }

    /// Insert TabSeparated data into a table.
    /// Format: each row on a new line, columns separated by tabs.
    pub async fn insert_tab_separated(&self, table: &str, data: impl Into<String>) -> Result<()> {
        let req = self
            .request(reqwest::Method::POST, "/")
            .query(&[(
                "query",
                &format!("INSERT INTO {} FORMAT TabSeparated", table),
            )])
            .header("Content-Type", "text/plain")
            .body(data.into());
        let res = req.send().await.context("clickhouse insert")?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow!("clickhouse insert failed ({status}): {text}"));
        }
        Ok(())
    }

    /// Get the highest block number in the given table.
    pub async fn max_block_num(&self, table: &str) -> Result<Option<u32>> {
        let sql = format!("SELECT max(block_num) FROM {} FINAL", table);
        let result = self.query(&sql).await?;
        let trimmed = result.trim();
        if trimmed.is_empty() || trimmed == "0" {
            return Ok(None);
        }
        Ok(trimmed.parse().ok())
    }

    /// Get the checkpoint block number from progress table.
    pub async fn get_checkpoint(&self) -> Result<Option<u32>> {
        // `FINAL` must immediately follow the table name, before `WHERE` -
        // confirmed against a real server (`FINAL` after `WHERE` is a
        // SYNTAX_ERROR, it doesn't just get ignored).
        let sql = "SELECT block_num FROM progress FINAL WHERE id = 'checkpoint' ORDER BY version DESC LIMIT 1";
        let result = self.query(sql).await?;
        let trimmed = result.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        Ok(trimmed.parse().ok())
    }

    /// Set the checkpoint block number in progress table.
    pub async fn set_checkpoint(&self, block_num: u32, version: u64) -> Result<()> {
        let sql = format!(
            "INSERT INTO progress (id, block_num, version) VALUES ('checkpoint', {}, {})",
            block_num, version
        );
        self.execute(&sql).await
    }

    /// Count documents in a table.
    pub async fn count(&self, table: &str) -> Result<u64> {
        let sql = format!("SELECT count() FROM {} FINAL", table);
        let result = self.query(&sql).await?;
        let trimmed = result.trim();
        trimmed.parse().context("invalid count response")
    }

    /// Drop all tables for clean restart.
    pub async fn drop_all(&self) -> Result<()> {
        for table in &[
            "action", "block", "delta", "abi", "perm", "token", "progress",
        ] {
            let sql = format!("DROP TABLE IF EXISTS {}", table);
            let _ = self.execute(&sql).await;
        }
        Ok(())
    }

    /// Create all tables from schema.
    pub async fn create_all(&self) -> Result<()> {
        use crate::clickhouse::schema::create_tables_sql;
        // Split on the statement terminator, not on the literal "CREATE
        // TABLE" text: `create_tables_sql()` puts a `-- comment` line before
        // every statement, so splitting on "CREATE TABLE" turns each leading
        // comment into its own fragment, which this then re-glued into
        // "CREATE TABLE\n-- comment\n" - a statement with no table name.
        // Confirmed against a real server: that is a SQL syntax error, not
        // just untidy.
        for statement in create_tables_sql().split(';') {
            let statement = statement.trim();
            // Defends against the same class of bug a second way: a `--`
            // comment containing its own literal `;` splits into a
            // comment-only fragment here, which ClickHouse rejects as an
            // "Empty query" (comments carry no statement). Belt-and-braces
            // alongside just not doing that in the DDL text above.
            let has_statement = statement
                .lines()
                .any(|line| !line.trim().is_empty() && !line.trim_start().starts_with("--"));
            if !has_statement {
                continue;
            }
            self.execute(statement).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // requires running ClickHouse
    async fn test_ping() {
        let ck = ClickHouse::new("http://localhost:8123", None, None);
        assert!(ck.ping().await.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_query() {
        let ck = ClickHouse::new("http://localhost:8123", None, None);
        let result = ck.query("SELECT 1").await.unwrap();
        assert_eq!(result.trim(), "1");
    }
}
