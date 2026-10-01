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
            return Err(anyhow!("clickhouse {method} {path} failed ({status}): {text}"));
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
            Err(anyhow!("clickhouse ping failed with status {}", res.status()))
        }
    }

    /// Execute a raw SQL query. Returns the response as text.
    pub async fn query(&self, sql: &str) -> Result<String> {
        let req = self
            .request(reqwest::Method::GET, "/")
            .query(&[("query", sql)])
            .header("Accept", "text/plain");
        let res = req
            .send()
            .await
            .context("clickhouse query")?;
        let status = res.status();
        let text = res.text().await.context("clickhouse response body")?;
        if !status.is_success() {
            return Err(anyhow!("clickhouse query failed ({status}): {text}"));
        }
        Ok(text)
    }

    /// Execute SQL without returning results (DDL, CREATE, etc).
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.query(sql).await?;
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
        let sql = format!(
            "SELECT max(block_num) FROM {} FINAL",
            table
        );
        let result = self.query(&sql).await?;
        let trimmed = result.trim();
        if trimmed.is_empty() || trimmed == "0" {
            return Ok(None);
        }
        Ok(trimmed.parse().ok())
    }

    /// Get the checkpoint block number from progress table.
    pub async fn get_checkpoint(&self) -> Result<Option<u32>> {
        let sql = "SELECT block_num FROM progress WHERE id = 'checkpoint' FINAL ORDER BY version DESC LIMIT 1";
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
        for table in &["action", "block", "delta", "abi", "perm", "token", "progress"] {
            let sql = format!("DROP TABLE IF EXISTS {}", table);
            let _ = self.execute(&sql).await;
        }
        Ok(())
    }

    /// Create all tables from schema.
    pub async fn create_all(&self) -> Result<()> {
        use crate::clickhouse::schema::create_tables_sql;
        // Split by CREATE TABLE and execute each
        for statement in create_tables_sql().split("CREATE TABLE") {
            if statement.trim().is_empty() {
                continue;
            }
            let sql = format!("CREATE TABLE{}", statement);
            self.execute(sql.trim()).await?;
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
