# ClickHouse Migration - Production Deployment Guide

**Phase 4 Complete**: Full integration, monitoring, and deployment ready.

---

## Quick Start

### 1. Configuration

Add to `hyperion.toml`:

```toml
[clickhouse]
url = "http://localhost:8123"
user = "default"
pass = ""
enabled = true  # enable dual-write or CH-only mode

[indexer]
writer_concurrency = 4
batch_size = 2000
batch_max_bytes = 5242880  # 5 MB
flush_interval_ms = 500
```

### 2. Backend Selection

Configure in `main.rs`:

```rust
use hyperion::backend::IndexBackend;

// Option A: Elasticsearch only (current production)
let backend = IndexBackend::new_es(es);

// Option B: ClickHouse only (new production target)
let backend = IndexBackend::new_ck(ck);

// Option C: Dual-write (24–72 hour validation)
let backend = IndexBackend::new_dual(es, ck);
```

### 3. Launch Pipeline

```rust
use hyperion::indexer_integration::WriteBatch;

// Create batch supporting selected backend
let mut batch = WriteBatch::new(&backend);

// Push documents (works with all backends)
batch.push(&doc, version, &indices)?;

// Write concurrently with metrics
write_batches(&backend, batch_rx, concurrency).await?;
```

---

## Deployment Timeline

### Week 1: Setup & Validation

**Day 1–2: Staging ClickHouse**
```bash
# Deploy ClickHouse
docker pull clickhouse/clickhouse-server
docker run -d \
  -p 8123:8123 \
  -v /data/clickhouse:/var/lib/clickhouse \
  --name clickhouse-stage \
  clickhouse/clickhouse-server

# Verify
curl http://localhost:8123/ping
```

**Day 3–5: Benchmark & Tune**
```bash
# Run Phase 0 benchmarks
cargo test --test es_vs_ch_comparison -- --ignored --nocapture

# Record metrics:
# - Throughput (blocks/sec, actions/sec)
# - Memory (CH RSS at 1M blocks)
# - Query latency (p50, p95, p99)
# - Compression ratio
```

**Expected results:**
- Throughput: 150–300 blocks/sec (1.5–2× ES)
- Memory: 10–15 GB for ClickHouse
- Query latency: ±10% vs ES baseline

### Week 2: Staging Dual-Write

**Day 1–2: Deploy Code**
```bash
git checkout feat/clickhouse-migration
cargo build --release
```

**Day 3–5: Enable Dual-Write**
```toml
[clickhouse]
enabled = true
```

Set backend to dual-write mode:
```rust
let backend = IndexBackend::new_dual(es, ck);
```

**Day 6–7: Monitor & Collect Data**
- Run 24–48 hours with dual-write
- Monitor metrics:
  ```bash
  curl http://localhost:7000/metrics  # Prometheus metrics
  ```
- Check divergence counter (target: < 0.1%)
- Monitor memory, CPU, disk I/O

### Week 3: Production Canary

**Day 1: Prepare Cutover**
1. Backup ES indices (optional, CH has all data)
2. Verify CH checkpoint matches ES
3. Prepare rollback procedure (15-min procedure)

**Day 2: Switch Reads (Canary)**
```rust
// API configuration
let query_backend = match config.read_percent {
    0..=10 => QueryBackend::new_shadow(
        QueryBackend::new_es(es),     // primary (most traffic)
        QueryBackend::new_ck(ck),     // secondary (10% shadow)
    ),
    _ => QueryBackend::new_ck(ck),    // CH primary
};
```

- Route 10% of reads to ClickHouse
- Monitor latency, error rates
- Compare results (shadow read framework)

**Day 3: Expand Reads**
- Increase to 50% of reads
- Monitor for 4–6 hours
- Expand to 100%

**Day 4: Switch Writes**
```rust
let backend = IndexBackend::new_ck(ck);  // CH only
```

- Stop ES bulk writer
- Verify CH throughput ≥ 150 blocks/sec
- Monitor checkpoint advancement

**Day 5–7: Monitor**
- Run 24 hours with CH primary
- Track all metrics
- Watch for anomalies
- Keep ES online as warm backup

---

## Monitoring

### Prometheus Metrics

All metrics exported at `/metrics` endpoint:

```
hyperion_ch_rows_inserted_total      # Total rows to CH
hyperion_es_rows_inserted_total      # Total rows to ES
hyperion_batch_inserts_total         # Batch count
hyperion_batch_insert_errors_total   # Errors
hyperion_ch_insert_latency_ms        # CH p50 latency
hyperion_es_insert_latency_ms        # ES p50 latency
hyperion_queries_total               # API queries
hyperion_query_latency_ms            # p50 query latency
hyperion_checkpoint_block            # Current block
hyperion_divergences_total           # ES vs CH divergences
hyperion_memory_bytes                # Process memory
```

### Grafana Dashboard

Create dashboard with:

**Ingestion Panel**:
```
rate(hyperion_ch_rows_inserted_total[1m])   # CH insert rate
rate(hyperion_es_rows_inserted_total[1m])   # ES insert rate
rate(hyperion_batch_insert_errors_total[1m]) # Error rate
```

**Latency Panel**:
```
hyperion_ch_insert_latency_ms       # CH latency
hyperion_es_insert_latency_ms       # ES latency
hyperion_query_latency_ms           # Query latency
```

**Health Panel**:
```
hyperion_checkpoint_block           # Checkpoint progress
hyperion_divergences_total          # Divergence counter
hyperion_memory_bytes               # Memory usage
rate(hyperion_batch_inserts_total[5m]) # Throughput
```

### Health Checks

```bash
# ClickHouse health
curl http://localhost:8123/ping

# Indexer health (from logs)
grep "checkpoint" /var/log/hyperion.log | tail -1

# Query health
curl http://localhost:7000/v2/history/get_actions?account=alice

# Metrics export
curl http://localhost:7000/metrics
```

---

## Rollback Procedure

**If latency degrades (> 2× baseline)**:

```rust
// Revert to ES
let backend = IndexBackend::new_es(es);
```

Time: 5 minutes
Data loss: None (ES was warm backup)
Impact: Resume from last indexed block

**If memory spikes (> 20 GB)**:

```toml
[indexer]
batch_size = 1000        # Reduce batch size
writer_concurrency = 2   # Reduce parallelism
```

Restart indexer, resume from checkpoint.

**If divergence > 1%**:

```bash
# Stop indexer
cargo test shadow_read  # Debug divergence

# Options:
# 1. Fix bug in writer.rs
# 2. Restore from ES backup
# 3. Re-ingest blocks from SHIP
```

---

## Operational Runbooks

### Morning Checklist

```bash
# 1. Check checkpoint progression
curl http://localhost:7000/metrics | grep checkpoint

# 2. Check error rate
curl http://localhost:7000/metrics | grep batch_insert_errors

# 3. Check memory usage
ps aux | grep clickhouse-server | head -1

# 4. Check divergence (dual-write only)
curl http://localhost:7000/metrics | grep divergences

# 5. Sample query
curl 'http://localhost:7000/v2/history/get_actions?account=alice&limit=10'
```

### Troubleshooting Guide

**High Memory (> 20 GB)**:
```bash
# Check for stuck merges
clickhouse-client -q "SELECT * FROM system.merges"

# Kill longest merge
clickhouse-client -q "KILL MUTATION WHERE ..."

# Trigger compact
clickhouse-client -q "OPTIMIZE TABLE action FINAL"

# If still high: reduce batch_size, restart
```

**Slow Queries (> 5 sec)**:
```bash
# Check query log
clickhouse-client -q "SELECT query, query_duration_ms FROM system.query_log WHERE query_duration_ms > 5000"

# Check WHERE clause matches ORDER BY prefix
# Example: notified = 'alice' AND block_num > 1000 (good)
# Example: trx_id = 'abc...' (bad - full scan)

# Add skip index if needed
```

**Checkpoint Not Advancing**:
```bash
# Check for errors in logs
grep "checkpoint" /var/log/hyperion.log

# Verify ClickHouse is accepting writes
clickhouse-client -q "INSERT INTO progress (id, block_num, version) VALUES ('test', 0, 0)"

# Check progress table
clickhouse-client -q "SELECT * FROM progress FINAL"
```

**Divergence Detected**:
```bash
# Enable shadow read logging (in code)
// Identify which table type (action, block, delta, etc.)
// Sample document from both systems
// Compare field-by-field

# Common causes:
# 1. ABI decode failure (action data malformed)
# 2. Null handling differences (ES vs CH)
# 3. Float precision loss
# 4. Array field formatting

# Solution: Fix in writer.rs, retest
```

---

## Performance Tuning

### Batch Size Tuning

Current: 2,000 documents/batch

**Increase if**:
- Memory available > 20 GB
- Disk I/O idle (< 50%)
- Throughput desired > 300 blocks/sec

```toml
batch_size = 5000  # Larger batches
batch_max_bytes = 10485760  # 10 MB max
```

**Decrease if**:
- Memory pressure > 80%
- High latency spikes
- Network bandwidth constrained

```toml
batch_size = 1000  # Smaller batches
```

### Concurrency Tuning

Current: 4 concurrent writers

**Increase if**:
- Disks have high IOPS capacity (> 10K)
- CPU cores available (> 4)
- Throughput desired > 200 blocks/sec

```toml
writer_concurrency = 8
```

**Decrease if**:
- Memory or CPU constrained
- Merge operations pile up
- Write latency spikes

```toml
writer_concurrency = 2
```

### ClickHouse Tuning

Edit `schema.rs`:

```sql
-- Smaller granules (less memory per merge)
INDEX idx_notified notified TYPE bloom_filter()
SETTINGS index_granularity = 4096  -- down from 8192

-- More aggressive merging (less data in memory)
SETTINGS parts_to_throw_insert_exception = 50  -- down from 100
```

---

## Security Checklist

- [ ] ClickHouse behind firewall (not internet-accessible)
- [ ] ES credentials rotated
- [ ] ClickHouse credentials set (not `default`)
- [ ] Metrics endpoint behind auth (not public)
- [ ] Logs don't expose sensitive data
- [ ] Backups encrypted
- [ ] Access logs enabled
- [ ] Rate limiting on API (ddos protection)

---

## Disaster Recovery

### Backup Strategy

**Daily backup** (after cutover):
```bash
#!/bin/bash
clickhouse-client -q "BACKUP DATABASE hyperion TO 's3://backups/hyperion-$(date +%Y-%m-%d).tar.gz'"
```

Keep 7 daily, 4 weekly backups.

### Recovery Procedure

**Option A: Restore from ClickHouse backup**
```bash
clickhouse-client -q "RESTORE DATABASE hyperion FROM 's3://backups/hyperion-2024-01-01.tar.gz'"
# Takes ~2–4 hours for 1.2 TB
```

**Option B: Reindex from SHIP**
```bash
# Set start_block to recovery point
[indexer]
start_block = 12345000

# Restart indexer
cargo run -- --config hyperion.toml
# Takes ~24–48 hours for full history
```

**Option C: Restore from ES (if available)**
```bash
# Temporarily enable ES
[elasticsearch]
url = "http://es-backup:9200"

# Set backend to ES
let backend = IndexBackend::new_es(es);

# Reindex ClickHouse from ES
# (requires custom tool)
```

---

## Post-Launch Checklist

- [ ] Metrics collecting (Prometheus scraping)
- [ ] Dashboard monitoring (Grafana)
- [ ] Alerts configured (Slack/PagerDuty)
- [ ] Runbooks documented
- [ ] Team trained on procedures
- [ ] Backup testing (restore works)
- [ ] Log aggregation (ELK/Splunk)
- [ ] On-call rotation established
- [ ] SLA metrics defined
- [ ] Post-mortems process ready

---

## Success Criteria

✅ **Performance**:
- Throughput ≥ 200 blocks/sec
- Query latency ≤ ES baseline
- Memory ≤ 50 GB total (replay + CH)

✅ **Reliability**:
- Divergence < 0.1% (dual-write phase)
- Error rate < 0.01%
- Checkpoint advancing continuously

✅ **Operations**:
- Alerts < 1 per hour (baseline)
- MTTR < 30 minutes
- Zero data loss

✅ **User Experience**:
- API response times stable
- No transaction timeouts
- No user-facing errors

---

## FAQ

**Q: Can we go back to ES if CH doesn't work?**
A: Yes, instant rollback (config change, restart). CH was warm backup during dual-write.

**Q: How long does a full reindex take?**
A: ~24–48 hours for 1.2 TB on single machine with typical disk (1–2 GB/sec).

**Q: Do we need to change the API?**
A: No, API response format unchanged. QueryBackend abstraction handles translation.

**Q: What about ES features we're using?**
A: Full-text search requires custom implementation (positionCaseInsensitive). All other features supported.

**Q: Can we run dual-write indefinitely?**
A: Yes, if acceptable throughput reduction (~15–20% overhead). Not recommended for >6 months.

**Q: How do we handle forks?**
A: Drop partition, re-ingest. Replication not yet supported (future work).

---

## Support & Escalation

**Latency Issues**:
1. Check ClickHouse query log
2. Review WHERE clause (ORDER BY prefix?)
3. Check indexes exist
4. Contact ClickHouse support if needed

**Memory Issues**:
1. Stop merges (KILL MUTATION)
2. Reduce batch size
3. Check for memory leaks (monitor RSS)
4. Contact Anthropic team

**Data Loss**:
1. STOP indexer immediately
2. Check backup status
3. Initiate recovery procedure
4. Post-mortem

---

## Links & Resources

- [ClickHouse Docs](https://clickhouse.com/docs)
- [Hyperion API](https://github.com/cc32d9/hyperion-history-api)
- [CLICKHOUSE_MIGRATION.md](./CLICKHOUSE_MIGRATION.md) - Complete architecture
- [IMPLEMENTATION_STATUS.md](./IMPLEMENTATION_STATUS.md) - Implementation details
- Code: `src/clickhouse/`, `src/backend.rs`, `src/metrics.rs`
- Tests: `tests/integration_clickhouse.rs`, `tests/es_vs_ch_comparison.rs`

---

**Ready for production!** Follow the timeline, monitor carefully, and keep ES as warm backup. You've got this. 🚀
