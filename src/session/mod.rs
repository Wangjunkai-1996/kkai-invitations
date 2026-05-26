pub mod gob;
pub mod securecookie;

use std::collections::HashMap;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SessionUser {
    pub id: i64,
    pub username: String,
    pub role: i64,
    pub status: i64,
    pub group: String,
}

impl SessionUser {
    pub fn is_admin(&self) -> bool {
        self.role >= 10
    }

    pub fn from_session_map(map: &HashMap<String, gob::GobValue>) -> Option<Self> {
        let id = match map.get("id")? {
            gob::GobValue::Int(v) => *v,
            _ => return None,
        };
        let username = match map.get("username")? {
            gob::GobValue::String(v) => v.clone(),
            _ => return None,
        };
        let role = match map.get("role")? {
            gob::GobValue::Int(v) => *v,
            _ => return None,
        };
        let status = match map.get("status")? {
            gob::GobValue::Int(v) => *v,
            _ => return None,
        };
        let group = match map.get("group")? {
            gob::GobValue::String(v) => v.clone(),
            _ => return None,
        };
        Some(Self {
            id,
            username,
            role,
            status,
            group,
        })
    }
}
