//! Public key and signature decoding from the Antelope binary formats into
//! the canonical `PUB_K1_...` / `SIG_K1_...` string representations.

use crate::{AntelopeError, ByteReader, Result};
use ripemd::{Digest, Ripemd160};

fn checksummed_base58(prefix: &str, suffix: &str, payload: &[u8]) -> String {
    let mut hasher = Ripemd160::new();
    hasher.update(payload);
    hasher.update(suffix.as_bytes());
    let digest = hasher.finalize();
    let mut data = payload.to_vec();
    data.extend_from_slice(&digest[..4]);
    format!("{prefix}{suffix}_{}", bs58::encode(data).into_string())
}

fn key_type(tag: u8) -> Result<&'static str> {
    match tag {
        0 => Ok("K1"),
        1 => Ok("R1"),
        2 => Ok("WA"),
        other => Err(AntelopeError::BadKeyType(other)),
    }
}

/// Read a binary `public_key` and render it as `PUB_<type>_...`.
pub fn read_public_key(reader: &mut ByteReader) -> Result<String> {
    let tag = reader.read_u8()?;
    let suffix = key_type(tag)?;
    let payload: Vec<u8> = match tag {
        0 | 1 => reader.read_exact(33)?.to_vec(),
        _ => {
            // WA key: 33-byte compressed point + user_presence + rpid string.
            let start = reader.pos();
            reader.read_exact(33)?;
            reader.read_u8()?;
            let rpid_len = reader.read_varuint32()? as usize;
            reader.read_exact(rpid_len)?;
            reader.data_slice(start, reader.pos()).to_vec()
        }
    };
    Ok(checksummed_base58("PUB_", suffix, &payload))
}

/// Read a binary `signature` and render it as `SIG_<type>_...`.
pub fn read_signature(reader: &mut ByteReader) -> Result<String> {
    let tag = reader.read_u8()?;
    let suffix = key_type(tag)?;
    let payload: Vec<u8> = match tag {
        0 | 1 => reader.read_exact(65)?.to_vec(),
        _ => {
            // WA signature: compact sig + auth_data bytes + client_json string.
            let start = reader.pos();
            reader.read_exact(65)?;
            let auth_len = reader.read_varuint32()? as usize;
            reader.read_exact(auth_len)?;
            let json_len = reader.read_varuint32()? as usize;
            reader.read_exact(json_len)?;
            reader.data_slice(start, reader.pos()).to_vec()
        }
    };
    Ok(checksummed_base58("SIG_", suffix, &payload))
}

/// Normalize a public key string to the `PUB_K1_...` form used in indexed
/// documents. Legacy keys (`EOS...` and other chain prefixes) carry the same
/// 33-byte payload with a suffix-less checksum; re-encode them. Keys already
/// in `PUB_` form pass through unchanged.
pub fn normalize_public_key(key: &str) -> String {
    if key.starts_with("PUB_") {
        return key.to_string();
    }
    // Legacy prefix is 1..4 alphabetic chars (EOS, WAX, FIO, ...). Try the
    // known common ones, longest first.
    for prefix_len in (2..=4).rev() {
        if key.len() <= prefix_len {
            continue;
        }
        let (prefix, body) = key.split_at(prefix_len);
        if !prefix.chars().all(|c| c.is_ascii_alphabetic()) {
            continue;
        }
        let Ok(data) = bs58::decode(body).into_vec() else {
            continue;
        };
        if data.len() != 37 {
            continue;
        }
        let payload = &data[..33];
        let mut hasher = Ripemd160::new();
        hasher.update(payload);
        let digest = hasher.finalize();
        if digest[..4] != data[33..] {
            continue;
        }
        return checksummed_base58("PUB_", "K1", payload);
    }
    key.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn k1_key_known_vector() {
        // abieos test vector: the all-zeros K1 key (legacy form
        // EOS1111111111111111111111111111111114T1Anm).
        let mut raw = vec![0u8]; // K1 tag
        raw.extend_from_slice(&[0u8; 33]);
        let mut r = ByteReader::new(&raw);
        let key = read_public_key(&mut r).unwrap();
        assert_eq!(key, "PUB_K1_11111111111111111111111111111111149Mr2R");
        assert!(r.is_empty());
    }

    #[test]
    fn normalizes_legacy_key() {
        // Legacy form of the all-zeros key (abieos vector).
        assert_eq!(
            normalize_public_key("EOS1111111111111111111111111111111114T1Anm"),
            "PUB_K1_11111111111111111111111111111111149Mr2R"
        );
        // Already-normalized and garbage inputs pass through.
        assert_eq!(normalize_public_key("PUB_K1_abc"), "PUB_K1_abc");
        assert_eq!(normalize_public_key("nonsense"), "nonsense");
    }

    #[test]
    fn rejects_unknown_tag() {
        let raw = [9u8; 34];
        let mut r = ByteReader::new(&raw);
        assert!(read_public_key(&mut r).is_err());
    }
}
