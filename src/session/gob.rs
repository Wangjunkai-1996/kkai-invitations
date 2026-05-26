use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum GobValue {
    Int(i64),
    String(String),
}

#[derive(Debug, Error)]
pub enum GobError {
    #[error("unexpected end of data")]
    UnexpectedEof,
    #[error("invalid format: {0}")]
    InvalidFormat(String),
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_byte(&mut self) -> Result<u8, GobError> {
        if self.pos >= self.data.len() {
            return Err(GobError::UnexpectedEof);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], GobError> {
        if self.pos + n > self.data.len() {
            return Err(GobError::UnexpectedEof);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_uint(&mut self) -> Result<u64, GobError> {
        let b = self.read_byte()?;
        if b <= 0x7f {
            return Ok(b as u64);
        }
        let n = -(b as i8) as usize;
        if n > 8 {
            return Err(GobError::InvalidFormat("uint too large".into()));
        }
        let mut val: u64 = 0;
        for _ in 0..n {
            val = (val << 8) | self.read_byte()? as u64;
        }
        Ok(val)
    }

    fn read_int(&mut self) -> Result<i64, GobError> {
        let x = self.read_uint()?;
        if x & 1 != 0 {
            Ok(!(x >> 1) as i64)
        } else {
            Ok((x >> 1) as i64)
        }
    }

    fn read_string(&mut self) -> Result<String, GobError> {
        let len = self.read_uint()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| GobError::InvalidFormat("invalid utf8 string".into()))
    }
}

pub fn decode_session_map(data: &[u8]) -> Result<HashMap<String, GobValue>, GobError> {
    let mut reader = Reader::new(data);

    // Skip type definition messages (negative type ids)
    loop {
        let msg_len = reader.read_uint()? as usize;
        if msg_len == 0 || reader.remaining() < msg_len {
            return Err(GobError::UnexpectedEof);
        }

        let msg_start = reader.pos;
        let type_id = reader.read_int()?;

        if type_id >= 0 {
            // This is the data message - parse the map
            let consumed = reader.pos - msg_start;
            return decode_map_data(&mut reader, msg_len - consumed);
        }

        // Skip this type definition message
        reader.pos = msg_start + msg_len;
    }
}

fn decode_map_data(
    reader: &mut Reader,
    _remaining: usize,
) -> Result<HashMap<String, GobValue>, GobError> {
    // Skip singleton field delta (always 0 for top-level values)
    let delta = reader.read_uint()?;
    if delta != 0 {
        return Err(GobError::InvalidFormat(format!(
            "expected top-level singleton delta 0, got {}",
            delta
        )));
    }

    let count = reader.read_uint()? as usize;
    let mut map = HashMap::with_capacity(count);

    for _ in 0..count {
        let key = decode_interface_value(reader)?;
        let value = decode_interface_value(reader)?;

        let key_str = match key {
            GobValue::String(s) => s,
            _ => return Err(GobError::InvalidFormat("map key must be string".into())),
        };
        map.insert(key_str, value);
    }

    Ok(map)
}

fn decode_interface_value(reader: &mut Reader) -> Result<GobValue, GobError> {
    // 1. Read type name
    let type_name = reader.read_string()?;

    // 2. Read type id (zigzag encoded)
    let _type_id = reader.read_uint()?;

    // 3. Read value byte count
    let value_len = reader.read_uint()? as usize;
    if reader.remaining() < value_len {
        return Err(GobError::UnexpectedEof);
    }

    // 4. Read singleton field delta (always 0)
    let delta = reader.read_byte()?;
    if delta != 0 {
        return Err(GobError::InvalidFormat(format!(
            "expected singleton delta 0, got {}",
            delta
        )));
    }

    // 5. Decode the actual value based on type name
    match type_name.as_str() {
        "string" => {
            let s = reader.read_string()?;
            Ok(GobValue::String(s))
        }
        "int" => {
            let v = reader.read_int()?;
            Ok(GobValue::Int(v))
        }
        other => Err(GobError::UnsupportedType(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_session_map() {
        let hex_str = "0d7f040102ff8000011001100000ff90ff80000506737472696e670c0a0008757365726e616d6506737472696e670c07000561646d696e06737472696e670c060004726f6c6503696e74040400fe07d006737472696e670c08000673746174757303696e740402000206737472696e670c07000567726f757006737472696e670c09000764656661756c7406737472696e670c040002696403696e7404020002";
        let data = hex::decode(hex_str).unwrap();

        let map = decode_session_map(&data).unwrap();

        assert_eq!(map.get("id"), Some(&GobValue::Int(1)));
        assert_eq!(map.get("username"), Some(&GobValue::String("admin".into())));
        assert_eq!(map.get("role"), Some(&GobValue::Int(1000)));
        assert_eq!(map.get("status"), Some(&GobValue::Int(1)));
        assert_eq!(map.get("group"), Some(&GobValue::String("default".into())));
    }

    #[test]
    fn test_reader_uint() {
        // Single byte values (0-127)
        let mut r = Reader::new(&[0x05]);
        assert_eq!(r.read_uint().unwrap(), 5);

        // Multi-byte value: 0xff 0x90 = 144
        let mut r = Reader::new(&[0xff, 0x90]);
        assert_eq!(r.read_uint().unwrap(), 144);

        // Multi-byte value: 0xfe 0x07 0xd0 = 2000
        let mut r = Reader::new(&[0xfe, 0x07, 0xd0]);
        assert_eq!(r.read_uint().unwrap(), 2000);
    }

    #[test]
    fn test_reader_int() {
        // Zigzag: 2 -> 1
        let mut r = Reader::new(&[0x02]);
        assert_eq!(r.read_int().unwrap(), 1);

        // Zigzag: 4 -> 2
        let mut r = Reader::new(&[0x04]);
        assert_eq!(r.read_int().unwrap(), 2);

        // Zigzag: 0xfe 0x07 0xd0 = uint 2000 -> int 1000
        let mut r = Reader::new(&[0xfe, 0x07, 0xd0]);
        assert_eq!(r.read_int().unwrap(), 1000);

        // Negative: zigzag 1 -> -1
        let mut r = Reader::new(&[0x01]);
        assert_eq!(r.read_int().unwrap(), -1);
    }
}
