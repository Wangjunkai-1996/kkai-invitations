use base64::{engine::general_purpose::URL_SAFE, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

use super::gob::{self, GobValue};
use super::SessionUser;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("base64 decode failed: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("invalid cookie format: expected 3 pipe-separated parts")]
    InvalidFormat,
    #[error("HMAC verification failed")]
    HmacInvalid,
    #[error("invalid timestamp")]
    TimestampInvalid,
    #[error("session expired")]
    TimestampExpired,
    #[error("gob decode failed: {0}")]
    GobDecode(String),
    #[error("session data missing required fields")]
    MissingFields,
}

pub struct SecureCookieDecoder {
    hash_key: Vec<u8>,
    max_age: i64,
    cookie_name: String,
}

impl SecureCookieDecoder {
    pub fn new(hash_key: &[u8], cookie_name: &str) -> Self {
        Self {
            hash_key: hash_key.to_vec(),
            max_age: 86400 * 30,
            cookie_name: cookie_name.to_string(),
        }
    }

    pub fn decode(&self, cookie_value: &str) -> Result<SessionUser, DecodeError> {
        let map = self.decode_to_map(cookie_value)?;
        SessionUser::from_session_map(&map).ok_or(DecodeError::MissingFields)
    }

    pub fn decode_to_map(
        &self,
        cookie_value: &str,
    ) -> Result<HashMap<String, GobValue>, DecodeError> {
        // 1. Base64 URL decode the outer layer
        let decoded = URL_SAFE.decode(cookie_value)?;

        // 2. Split by '|' into 3 parts: date|value|mac
        let parts: Vec<&[u8]> = splitn_bytes(&decoded, b'|', 3);
        if parts.len() != 3 {
            return Err(DecodeError::InvalidFormat);
        }

        let date_part = parts[0];
        let value_part = parts[1];
        let mac_part = parts[2];

        // 3. Verify HMAC-SHA256
        // MAC message = "name|date|value"
        let mut mac_message =
            Vec::with_capacity(self.cookie_name.len() + 1 + date_part.len() + 1 + value_part.len());
        mac_message.extend_from_slice(self.cookie_name.as_bytes());
        mac_message.push(b'|');
        mac_message.extend_from_slice(date_part);
        mac_message.push(b'|');
        mac_message.extend_from_slice(value_part);

        let mut mac =
            HmacSha256::new_from_slice(&self.hash_key).expect("HMAC can take key of any size");
        mac.update(&mac_message);

        if mac.verify_slice(mac_part).is_err() {
            return Err(DecodeError::HmacInvalid);
        }

        // 4. Verify timestamp
        let timestamp_str =
            std::str::from_utf8(date_part).map_err(|_| DecodeError::TimestampInvalid)?;
        let timestamp: i64 = timestamp_str
            .parse()
            .map_err(|_| DecodeError::TimestampInvalid)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        if self.max_age != 0 && timestamp < now - self.max_age {
            return Err(DecodeError::TimestampExpired);
        }

        // 5. Base64 URL decode the value part
        let gob_bytes = URL_SAFE.decode(value_part)?;

        // 6. Gob decode
        let session_map = gob::decode_session_map(&gob_bytes)
            .map_err(|e| DecodeError::GobDecode(e.to_string()))?;

        Ok(session_map)
    }
}

fn splitn_bytes(data: &[u8], sep: u8, n: usize) -> Vec<&[u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    for (i, &byte) in data.iter().enumerate() {
        if byte == sep && parts.len() < n - 1 {
            parts.push(&data[start..i]);
            start = i + 1;
        }
    }
    parts.push(&data[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_session_cookie() {
        let secret = "test-session-secret-123";
        let cookie_value = "MTc3OTcyNDM3NnxEWDhFQVFMX2dBQUJFQUVRQUFEX2tQLUFBQVVHYzNSeWFXNW5EQVFBQW1sa0EybHVkQVFDQUFJR2MzUnlhVzVuREFvQUNIVnpaWEp1WVcxbEJuTjBjbWx1Wnd3SEFBVmhaRzFwYmdaemRISnBibWNNQmdBRWNtOXNaUU5wYm5RRUJBRC1COUFHYzNSeWFXNW5EQWdBQm5OMFlYUjFjd05wYm5RRUFnQUNCbk4wY21sdVp3d0hBQVZuY205MWNBWnpkSEpwYm1jTUNRQUhaR1ZtWVhWc2RBPT18JrOMCNzFyThCpUE0KIGpsvgE4HDxKjtQuWdXg_EN3jg=";

        let decoder = SecureCookieDecoder::new(secret.as_bytes(), "session");
        let result = decoder.decode_to_map(cookie_value);

        match &result {
            Ok(map) => {
                println!("Decoded session map: {:?}", map);
                assert_eq!(map.get("id"), Some(&GobValue::Int(1)));
                assert_eq!(
                    map.get("username"),
                    Some(&GobValue::String("admin".to_string()))
                );
                assert_eq!(map.get("role"), Some(&GobValue::Int(1000)));
                assert_eq!(map.get("status"), Some(&GobValue::Int(1)));
                assert_eq!(
                    map.get("group"),
                    Some(&GobValue::String("default".to_string()))
                );
            }
            Err(DecodeError::TimestampExpired) => {
                // Expected if test runs after cookie expires - still verify HMAC works
                println!("Cookie expired (expected in CI), testing without timestamp check");
                let decoder_no_age = SecureCookieDecoder {
                    hash_key: secret.as_bytes().to_vec(),
                    max_age: 0, // disable age check
                    cookie_name: "session".to_string(),
                };
                let map = decoder_no_age.decode_to_map(cookie_value).unwrap();
                assert_eq!(map.get("id"), Some(&GobValue::Int(1)));
                assert_eq!(
                    map.get("username"),
                    Some(&GobValue::String("admin".to_string()))
                );
                assert_eq!(map.get("role"), Some(&GobValue::Int(1000)));
                assert_eq!(map.get("status"), Some(&GobValue::Int(1)));
                assert_eq!(
                    map.get("group"),
                    Some(&GobValue::String("default".to_string()))
                );
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_decode_session_user() {
        let secret = "test-session-secret-123";
        let cookie_value = "MTc3OTcyNDM3NnxEWDhFQVFMX2dBQUJFQUVRQUFEX2tQLUFBQVVHYzNSeWFXNW5EQVFBQW1sa0EybHVkQVFDQUFJR2MzUnlhVzVuREFvQUNIVnpaWEp1WVcxbEJuTjBjbWx1Wnd3SEFBVmhaRzFwYmdaemRISnBibWNNQmdBRWNtOXNaUU5wYm5RRUJBRC1COUFHYzNSeWFXNW5EQWdBQm5OMFlYUjFjd05wYm5RRUFnQUNCbk4wY21sdVp3d0hBQVZuY205MWNBWnpkSEpwYm1jTUNRQUhaR1ZtWVhWc2RBPT18JrOMCNzFyThCpUE0KIGpsvgE4HDxKjtQuWdXg_EN3jg=";

        let decoder = SecureCookieDecoder {
            hash_key: secret.as_bytes().to_vec(),
            max_age: 0,
            cookie_name: "session".to_string(),
        };
        let user = decoder.decode(cookie_value).unwrap();
        assert_eq!(user.id, 1);
        assert_eq!(user.username, "admin");
        assert_eq!(user.role, 1000);
        assert_eq!(user.status, 1);
        assert_eq!(user.group, "default");
    }
}
