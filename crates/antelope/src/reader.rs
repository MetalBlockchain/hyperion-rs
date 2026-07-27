use crate::{AntelopeError, Name, Result};

/// A little-endian cursor over an Antelope-serialized byte buffer.
pub struct ByteReader<'a> {
    data: &'a [u8],
    pos: usize,
}

macro_rules! read_le {
    ($name:ident, $ty:ty) => {
        pub fn $name(&mut self) -> Result<$ty> {
            const N: usize = std::mem::size_of::<$ty>();
            let bytes = self.read_exact(N)?;
            Ok(<$ty>::from_le_bytes(
                bytes.try_into().expect("length checked"),
            ))
        }
    };
}

impl<'a> ByteReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        ByteReader { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Borrow an arbitrary span of the underlying buffer (does not move the
    /// cursor). Used to re-capture already-consumed multi-field payloads.
    pub fn data_slice(&self, start: usize, end: usize) -> &'a [u8] {
        &self.data[start..end]
    }

    pub fn read_exact(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(AntelopeError::Eof {
                needed: n,
                remaining: self.remaining(),
            });
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    read_le!(read_u8, u8);
    read_le!(read_u16, u16);
    read_le!(read_u32, u32);
    read_le!(read_u64, u64);
    read_le!(read_u128, u128);
    read_le!(read_i8, i8);
    read_le!(read_i16, i16);
    read_le!(read_i32, i32);
    read_le!(read_i64, i64);
    read_le!(read_i128, i128);
    read_le!(read_f32, f32);
    read_le!(read_f64, f64);

    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_varuint32(&mut self) -> Result<u32> {
        let mut value: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = self.read_u8()?;
            value |= ((byte & 0x7f) as u64) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                break;
            }
            if shift >= 35 {
                return Err(AntelopeError::BadVarint);
            }
        }
        u32::try_from(value).map_err(|_| AntelopeError::BadVarint)
    }

    pub fn read_varint32(&mut self) -> Result<i32> {
        let v = self.read_varuint32()?;
        // zigzag decode
        Ok(((v >> 1) as i32) ^ -((v & 1) as i32))
    }

    pub fn read_name(&mut self) -> Result<Name> {
        Ok(Name(self.read_u64()?))
    }

    /// Length-prefixed byte blob.
    pub fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_varuint32()? as usize;
        self.read_exact(len)
    }

    pub fn read_string(&mut self) -> Result<String> {
        let bytes = self.read_bytes()?;
        // Consoles and on-chain strings are not always valid UTF-8; be lossy
        // rather than dropping the whole trace.
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    pub fn read_checksum256(&mut self) -> Result<[u8; 32]> {
        Ok(self.read_exact(32)?.try_into().expect("length checked"))
    }

    pub fn read_checksum256_hex(&mut self) -> Result<String> {
        Ok(hex::encode(self.read_exact(32)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varuint_roundtrip() {
        let buf = [
            0x00, 0x7f, 0x80, 0x01, 0xb1, 0xc0, 0x01, 0xff, 0xff, 0xff, 0xff, 0x0f,
        ];
        let mut r = ByteReader::new(&buf);
        assert_eq!(r.read_varuint32().unwrap(), 0);
        assert_eq!(r.read_varuint32().unwrap(), 127);
        assert_eq!(r.read_varuint32().unwrap(), 128);
        assert_eq!(r.read_varuint32().unwrap(), 24625);
        assert_eq!(r.read_varuint32().unwrap(), u32::MAX);
        assert!(r.is_empty());
    }

    #[test]
    fn varint_zigzag() {
        // zigzag: 0 -> 0, 1 -> -1, 2 -> 1, 3 -> -2
        let buf = [0x00, 0x01, 0x02, 0x03];
        let mut r = ByteReader::new(&buf);
        assert_eq!(r.read_varint32().unwrap(), 0);
        assert_eq!(r.read_varint32().unwrap(), -1);
        assert_eq!(r.read_varint32().unwrap(), 1);
        assert_eq!(r.read_varint32().unwrap(), -2);
    }

    #[test]
    fn eof_is_error() {
        let mut r = ByteReader::new(&[1, 2]);
        assert!(r.read_u32().is_err());
    }
}
