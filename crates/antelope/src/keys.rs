//! Public key and signature decoding from the Antelope binary formats into
//! the canonical `PUB_<type>_...` / `SIG_<type>_...` string representations,
//! including PulseVM's ML-DSA-44, ML-DSA-65, and ML-DSA-87 variants.

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
        3 => Ok("MLDSA44"),
        4 => Ok("MLDSA65"),
        5 => Ok("MLDSA87"),
        other => Err(AntelopeError::BadKeyType(other)),
    }
}

/// Read a binary `public_key` and render it as `PUB_<type>_...`.
pub fn read_public_key(reader: &mut ByteReader) -> Result<String> {
    let tag = reader.read_u8()?;
    let suffix = key_type(tag)?;
    let payload = match tag {
        0 | 1 => reader.read_exact(33)?,
        3 => reader.read_exact(1312)?,
        4 => reader.read_exact(1952)?,
        5 => reader.read_exact(2592)?,
        2 => {
            // WA key: 33-byte compressed point + user_presence + rpid string.
            let start = reader.pos();
            reader.read_exact(33)?;
            reader.read_u8()?;
            let rpid_len = reader.read_varuint32()? as usize;
            reader.read_exact(rpid_len)?;
            reader.data_slice(start, reader.pos())
        }
        _ => unreachable!("key_type checked the tag"),
    };
    Ok(checksummed_base58("PUB_", suffix, payload))
}

/// Read a binary `signature` and render it as `SIG_<type>_...`.
pub fn read_signature(reader: &mut ByteReader) -> Result<String> {
    let tag = reader.read_u8()?;
    let suffix = key_type(tag)?;
    let payload = match tag {
        0 | 1 => reader.read_exact(65)?,
        // ML-DSA envelopes contain the public key followed by the detached
        // signature, with no length prefixes (PulseVM PR #116).
        3 => reader.read_exact(1312 + 2420)?,
        4 => reader.read_exact(1952 + 3309)?,
        5 => reader.read_exact(2592 + 4627)?,
        2 => {
            // WA signature: compact sig + auth_data bytes + client_json string.
            let start = reader.pos();
            reader.read_exact(65)?;
            let auth_len = reader.read_varuint32()? as usize;
            reader.read_exact(auth_len)?;
            let json_len = reader.read_varuint32()? as usize;
            reader.read_exact(json_len)?;
            reader.data_slice(start, reader.pos())
        }
        _ => unreachable!("key_type checked the tag"),
    };
    Ok(checksummed_base58("SIG_", suffix, payload))
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
    fn fixed_size_key_and_signature_vectors() {
        // Zero-payload checksum tails computed independently with Python's
        // hashlib RIPEMD-160 and integer-based base58 encoding.
        for (tag, suffix, key_len, sig_len, key_tail, sig_tail) in [
            (0, "K1", 33, 65, "49Mr2R", "6uk5ne"),
            (1, "R1", 33, 65, "6amPNj", "2eDT3A"),
            (3, "MLDSA44", 1312, 3732, "4pxTJf", "6nQj2T"),
            (4, "MLDSA65", 1952, 5261, "QM2i8", "4ogVNU"),
            (5, "MLDSA87", 2592, 7219, "6xdVrX", "2CHpCH"),
        ] {
            let mut raw = vec![tag];
            raw.resize(1 + key_len, 0);
            raw.push(42);
            let mut reader = ByteReader::new(&raw);
            let key = read_public_key(&mut reader).unwrap();
            assert_eq!(
                key,
                format!("PUB_{suffix}_{}{key_tail}", "1".repeat(key_len))
            );
            assert_eq!(normalize_public_key(&key), key);
            assert_eq!(reader.read_u8().unwrap(), 42);
            assert!(reader.is_empty());

            raw = vec![tag];
            raw.resize(1 + sig_len, 0);
            raw.push(42);
            let mut reader = ByteReader::new(&raw);
            assert_eq!(
                read_signature(&mut reader).unwrap(),
                format!("SIG_{suffix}_{}{sig_tail}", "1".repeat(sig_len))
            );
            assert_eq!(reader.read_u8().unwrap(), 42);
            assert!(reader.is_empty());
        }
    }

    #[test]
    fn ml_dsa_signatures_preserve_public_key_and_detached_signature() {
        // Independent checksum vectors with distinct key/signature bytes
        // catch omitted keys, included tags, or reordered envelope fields.
        for (tag, suffix, key_len, sig_len, checksum) in [
            (3, "MLDSA44", 1312, 2420, [0x11, 0xa7, 0xf7, 0x9b]),
            (4, "MLDSA65", 1952, 3309, [0xa4, 0x47, 0xe1, 0xd3]),
            (5, "MLDSA87", 2592, 4627, [0x05, 0xae, 0x88, 0x11]),
        ] {
            let mut raw = vec![tag];
            raw.resize(1 + key_len, 0x11);
            raw.resize(1 + key_len + sig_len, 0x22);
            let rendered = read_signature(&mut ByteReader::new(&raw)).unwrap();
            let prefix = format!("SIG_{suffix}_");
            let decoded = bs58::decode(rendered.strip_prefix(&prefix).unwrap())
                .into_vec()
                .unwrap();
            assert_eq!(&decoded[..key_len], &raw[1..1 + key_len]);
            assert_eq!(&decoded[key_len..key_len + sig_len], &raw[1 + key_len..]);
            assert_eq!(&decoded[key_len + sig_len..], &checksum);
        }
    }

    #[test]
    fn rejects_truncated_ml_dsa_payloads() {
        for (tag, key_len, sig_len) in [(3, 1312, 2420), (4, 1952, 3309), (5, 2592, 4627)] {
            let mut raw = vec![tag];
            raw.resize(1 + key_len + sig_len, 0);
            for len in [0, 1, key_len] {
                assert!(matches!(
                    read_public_key(&mut ByteReader::new(&raw[..len])),
                    Err(AntelopeError::Eof { .. })
                ));
            }
            for len in [0, 1, 1 + key_len, key_len + sig_len] {
                assert!(matches!(
                    read_signature(&mut ByteReader::new(&raw[..len])),
                    Err(AntelopeError::Eof { .. })
                ));
            }
        }
    }

    #[test]
    fn webauthn_vectors_preserve_variable_length_fields() {
        let mut raw = vec![2];
        raw.extend_from_slice(&[0x11; 33]);
        raw.extend_from_slice(&[1, 11]); // user presence, rpid length
        raw.extend_from_slice(b"example.com");
        raw.push(42);
        let mut reader = ByteReader::new(&raw);
        assert_eq!(
            read_public_key(&mut reader).unwrap(),
            "PUB_WA_DCPfnRuLETYSjYkMQeiHnn5ahJB17P8LhtTVund72ozYhrrj8QEwqgvob8k8fYf4m3kX"
        );
        assert_eq!(reader.read_u8().unwrap(), 42);
        assert!(reader.is_empty());

        let mut raw = vec![2];
        raw.extend_from_slice(&[0x22; 65]);
        raw.extend_from_slice(&[3, 1, 2, 3, 2]); // auth_data, client_json length
        raw.extend_from_slice(b"{}");
        raw.push(42);
        let mut reader = ByteReader::new(&raw);
        assert_eq!(
            read_signature(&mut reader).unwrap(),
            "SIG_WA_4JDSUuxmt2GLexS8ibCuZDKVfaK2WoLpByiKc4avUpW2bmKNmPQg5Tc9gKw6scZ6wAkHrbR5Ug28f8foJXiyNZsQch83gxZCcJmnoo1R"
        );
        assert_eq!(reader.read_u8().unwrap(), 42);
        assert!(reader.is_empty());
    }

    #[test]
    fn rejects_unknown_tag() {
        for tag in [6, 9, 0x83, 0x84, 0x85, 0xff] {
            // Continuation-bit encodings of ML-DSA tags are noncanonical.
            let raw = [tag; 34];
            assert!(matches!(
                read_public_key(&mut ByteReader::new(&raw)),
                Err(AntelopeError::BadKeyType(t)) if t == tag
            ));
            assert!(matches!(
                read_signature(&mut ByteReader::new(&raw)),
                Err(AntelopeError::BadKeyType(t)) if t == tag
            ));
        }
    }
}
