use crate::{AntelopeError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// An Antelope account/action/table name: up to 13 chars from
/// `.12345abcdefghijklmnopqrstuvwxyz`, base-32 packed into a u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Name(pub u64);

const CHARMAP: &[u8; 32] = b".12345abcdefghijklmnopqrstuvwxyz";

fn char_to_symbol(c: u8) -> Option<u64> {
    match c {
        b'a'..=b'z' => Some((c - b'a') as u64 + 6),
        b'1'..=b'5' => Some((c - b'1') as u64 + 1),
        b'.' => Some(0),
        _ => None,
    }
}

impl Name {
    pub const fn new(value: u64) -> Self {
        Name(value)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn to_string_lossy(value: u64) -> String {
        let mut chars = [b'.'; 13];
        let mut n = value;
        for i in 0..13 {
            let (mask, shift) = if i == 0 { (0x0f, 4) } else { (0x1f, 5) };
            chars[12 - i] = CHARMAP[(n & mask) as usize];
            n >>= shift;
        }
        let s = std::str::from_utf8(&chars).expect("charmap is ascii");
        s.trim_end_matches('.').to_string()
    }
}

impl FromStr for Name {
    type Err = AntelopeError;

    fn from_str(s: &str) -> Result<Self> {
        let bytes = s.as_bytes();
        if bytes.len() > 13 {
            return Err(AntelopeError::BadName(s.to_string()));
        }
        let mut value: u64 = 0;
        for (i, &c) in bytes.iter().enumerate() {
            let sym = char_to_symbol(c).ok_or_else(|| AntelopeError::BadName(s.to_string()))?;
            if i < 12 {
                value |= (sym & 0x1f) << (64 - 5 * (i + 1));
            } else {
                // The 13th character only has 4 bits of room.
                if sym > 0x0f {
                    return Err(AntelopeError::BadName(s.to_string()));
                }
                value |= sym;
            }
        }
        Ok(Name(value))
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&Name::to_string_lossy(self.0))
    }
}

impl Serialize for Name {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_known_names() {
        for s in [
            "eosio",
            "eosio.token",
            "transfer",
            "a",
            "zzzzzzzzzzzzj",
            "hezdimbshege",
            ".",
            "",
        ] {
            let trimmed = s.trim_end_matches('.');
            let n: Name = s.parse().unwrap();
            assert_eq!(n.to_string(), trimmed, "roundtrip failed for {s:?}");
        }
    }

    #[test]
    fn known_values() {
        assert_eq!("eosio".parse::<Name>().unwrap().0, 6138663577826885632);
        assert_eq!(
            "eosio.token".parse::<Name>().unwrap().0,
            6138663591592764928
        );
        assert_eq!("transfer".parse::<Name>().unwrap().0, 14829575313431724032);
        assert_eq!(Name(0).to_string(), "");
    }

    #[test]
    fn rejects_invalid() {
        assert!("EOSIO".parse::<Name>().is_err());
        assert!("toolongname havespace".parse::<Name>().is_err());
        assert!("aaaaaaaaaaaaaa".parse::<Name>().is_err()); // 14 chars
        assert!("zzzzzzzzzzzzz".parse::<Name>().is_err()); // 13th char > 'j'
    }
}
