use crate::{AntelopeError, Result};
use std::fmt;
use std::str::FromStr;

/// A 7-char-max uppercase token code, packed into a u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SymbolCode(pub u64);

impl SymbolCode {
    pub fn from_u64(v: u64) -> Self {
        SymbolCode(v)
    }
}

impl fmt::Display for SymbolCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v = self.0;
        while v > 0 {
            let c = (v & 0xff) as u8;
            if c == 0 {
                break;
            }
            f.write_str(std::str::from_utf8(&[c]).map_err(|_| fmt::Error)?)?;
            v >>= 8;
        }
        Ok(())
    }
}

impl FromStr for SymbolCode {
    type Err = AntelopeError;

    fn from_str(s: &str) -> Result<Self> {
        if s.is_empty() || s.len() > 7 || !s.bytes().all(|c| c.is_ascii_uppercase()) {
            return Err(AntelopeError::BadSymbol(s.to_string()));
        }
        let mut v: u64 = 0;
        for (i, c) in s.bytes().enumerate() {
            v |= (c as u64) << (8 * i);
        }
        Ok(SymbolCode(v))
    }
}

/// Symbol = precision (low byte) + symbol code (upper 7 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Symbol(pub u64);

impl Symbol {
    pub fn from_u64(v: u64) -> Self {
        Symbol(v)
    }

    pub fn precision(&self) -> u8 {
        (self.0 & 0xff) as u8
    }

    pub fn code(&self) -> SymbolCode {
        SymbolCode(self.0 >> 8)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{},{}", self.precision(), self.code())
    }
}

impl FromStr for Symbol {
    type Err = AntelopeError;

    fn from_str(s: &str) -> Result<Self> {
        let (prec, code) = s
            .split_once(',')
            .ok_or_else(|| AntelopeError::BadSymbol(s.to_string()))?;
        let precision: u8 = prec
            .parse()
            .map_err(|_| AntelopeError::BadSymbol(s.to_string()))?;
        let code: SymbolCode = code.parse()?;
        Ok(Symbol((code.0 << 8) | precision as u64))
    }
}

/// A token amount: i64 raw units + symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Asset {
    pub amount: i64,
    pub symbol: Symbol,
}

impl Asset {
    pub fn new(amount: i64, symbol: Symbol) -> Self {
        Asset { amount, symbol }
    }

    /// Amount as a float, for indexing convenience. Precision loss is
    /// acceptable here: the exact string form is preserved separately.
    pub fn to_f64(&self) -> f64 {
        self.amount as f64 / 10f64.powi(self.symbol.precision() as i32)
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let precision = self.symbol.precision() as usize;
        let negative = self.amount < 0;
        let abs = self.amount.unsigned_abs();
        let sign = if negative { "-" } else { "" };
        if precision == 0 {
            write!(f, "{sign}{abs} {}", self.symbol.code())
        } else {
            let divisor = 10u64.pow(precision as u32);
            let int = abs / divisor;
            let frac = abs % divisor;
            write!(f, "{sign}{int}.{frac:0precision$} {}", self.symbol.code())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_roundtrip() {
        let s: Symbol = "4,EOS".parse().unwrap();
        assert_eq!(s.precision(), 4);
        assert_eq!(s.code().to_string(), "EOS");
        assert_eq!(s.to_string(), "4,EOS");
        // 4,EOS well-known raw value
        assert_eq!(s.0, 1397703940);
    }

    #[test]
    fn asset_formatting() {
        let sym: Symbol = "4,EOS".parse().unwrap();
        assert_eq!(Asset::new(10000, sym).to_string(), "1.0000 EOS");
        assert_eq!(Asset::new(-5, sym).to_string(), "-0.0005 EOS");
        let zero: Symbol = "0,SYS".parse().unwrap();
        assert_eq!(Asset::new(42, zero).to_string(), "42 SYS");
    }
}
