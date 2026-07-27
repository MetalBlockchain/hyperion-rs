//! Antelope timestamp conversions. All chain timestamps are rendered in the
//! canonical `%Y-%m-%dT%H:%M:%S%.3f` form (UTC, no `Z` suffix) that nodeos
//! and Hyperion use.

use chrono::{DateTime, TimeZone, Utc};

/// Milliseconds between the Unix epoch and the block-timestamp epoch
/// (2000-01-01T00:00:00.000Z).
pub const BLOCK_TIMESTAMP_EPOCH_MS: i64 = 946_684_800_000;
/// Each block-timestamp slot is 500 ms.
pub const BLOCK_INTERVAL_MS: i64 = 500;

fn format(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
}

/// `time_point`: microseconds since the Unix epoch.
pub fn time_point_to_string(micros: i64) -> String {
    format(Utc.timestamp_micros(micros).single().unwrap_or_default())
}

/// `time_point_sec`: seconds since the Unix epoch.
pub fn time_point_sec_to_string(secs: u32) -> String {
    format(
        Utc.timestamp_opt(secs as i64, 0)
            .single()
            .unwrap_or_default(),
    )
}

/// `block_timestamp_type`: 500 ms slots since 2000-01-01.
pub fn block_timestamp_to_string(slot: u32) -> String {
    let ms = BLOCK_TIMESTAMP_EPOCH_MS + slot as i64 * BLOCK_INTERVAL_MS;
    format(Utc.timestamp_millis_opt(ms).single().unwrap_or_default())
}

/// Milliseconds since the Unix epoch for a block-timestamp slot.
pub fn block_timestamp_to_ms(slot: u32) -> i64 {
    BLOCK_TIMESTAMP_EPOCH_MS + slot as i64 * BLOCK_INTERVAL_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_timestamp_epoch() {
        assert_eq!(block_timestamp_to_string(0), "2000-01-01T00:00:00.000");
        assert_eq!(block_timestamp_to_string(1), "2000-01-01T00:00:00.500");
    }

    #[test]
    fn time_points() {
        assert_eq!(time_point_sec_to_string(0), "1970-01-01T00:00:00.000");
        assert_eq!(time_point_to_string(1_500_000), "1970-01-01T00:00:01.500");
    }
}
