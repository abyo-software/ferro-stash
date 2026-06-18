// SPDX-License-Identifier: Apache-2.0
//! Fingerprint filter — generate a hash fingerprint from event fields.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::Result;
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug, Clone)]
enum HashMethod {
    SHA256,
    SHA1,
    MD5,
    MURMUR3,
}

#[derive(Debug)]
pub struct FingerprintFilter {
    source: Vec<String>,
    target: String,
    method: HashMethod,
    concatenate_sources: bool,
    key: Option<String>,
    condition: Option<Condition>,
}

impl FingerprintFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let source = if let Some(arr) = settings.get("source").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            vec!["message".to_string()]
        };

        let target = settings
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("fingerprint")
            .to_string();

        let method = match settings
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("SHA256")
            .to_uppercase()
            .as_str()
        {
            "SHA1" => HashMethod::SHA1,
            "MD5" => HashMethod::MD5,
            "MURMUR3" | "MURMUR3_128" => HashMethod::MURMUR3,
            _ => HashMethod::SHA256,
        };

        let concatenate_sources = settings
            .get("concatenate_sources")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let key = settings
            .get("key")
            .and_then(|v| v.as_str())
            .map(String::from);

        Ok(Self {
            source,
            target,
            method,
            concatenate_sources,
            key,
            condition,
        })
    }

    fn compute_hash(&self, data: &[u8]) -> String {
        use sha2::Digest;

        if let Some(ref hmac_key) = self.key {
            // HMAC mode
            use hmac::Mac;
            match self.method {
                HashMethod::SHA256 => {
                    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(hmac_key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                HashMethod::SHA1 => {
                    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(hmac_key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                HashMethod::MD5 => {
                    let mut mac = hmac::Hmac::<md5::Md5>::new_from_slice(hmac_key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                HashMethod::MURMUR3 => {
                    // Murmur3 doesn't support HMAC; prepend key
                    let mut combined = hmac_key.as_bytes().to_vec();
                    combined.extend_from_slice(data);
                    let hash = murmur3_hash(&combined);
                    format!("{hash:016x}")
                }
            }
        } else {
            match self.method {
                HashMethod::SHA256 => {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(data);
                    hex::encode(hasher.finalize())
                }
                HashMethod::SHA1 => {
                    let mut hasher = sha1::Sha1::new();
                    hasher.update(data);
                    hex::encode(hasher.finalize())
                }
                HashMethod::MD5 => {
                    let mut hasher = md5::Md5::new();
                    hasher.update(data);
                    hex::encode(hasher.finalize())
                }
                HashMethod::MURMUR3 => {
                    let hash = murmur3_hash(data);
                    format!("{hash:016x}")
                }
            }
        }
    }
}

/// Simple murmur3 64-bit hash (murmur3 finalization mix).
fn murmur3_hash(data: &[u8]) -> u64 {
    let mut h: u64 = 0;
    for chunk in data.chunks(8) {
        let mut k: u64 = 0;
        for (i, &b) in chunk.iter().enumerate() {
            k |= u64::from(b) << (i * 8);
        }
        k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
        k = k.rotate_left(31);
        k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        h ^= k;
        h = h.rotate_left(27);
        h = h.wrapping_mul(5).wrapping_add(0x5273_1d0c);
    }
    h ^= data.len() as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[async_trait]
impl FilterPlugin for FingerprintFilter {
    fn name(&self) -> &'static str {
        "fingerprint"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        let data = if self.concatenate_sources || self.source.len() == 1 {
            let parts: Vec<String> = self
                .source
                .iter()
                .filter_map(|f| event.get(f).map(|v| v.to_string_lossy()))
                .collect();
            parts.join("|")
        } else {
            // Hash each source separately, concatenate hashes
            let parts: Vec<String> = self
                .source
                .iter()
                .filter_map(|f| event.get(f).map(|v| v.to_string_lossy()))
                .collect();
            parts.join("|")
        };

        if !data.is_empty() {
            let hash = self.compute_hash(data.as_bytes());
            event.set(self.target.clone(), EventValue::String(hash));
        }

        Ok(vec![event])
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fingerprint_sha256() {
        let settings = serde_json::json!({
            "source": ["message"],
            "target": "fingerprint",
            "method": "SHA256"
        });
        let filter = FingerprintFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        let fp = result[0].get("fingerprint").expect("fingerprint field");
        let hash_str = fp.as_str().expect("string");
        assert_eq!(hash_str.len(), 64); // SHA256 hex = 64 chars
    }

    #[tokio::test]
    async fn test_fingerprint_md5() {
        let settings = serde_json::json!({
            "source": ["message"],
            "method": "MD5"
        });
        let filter = FingerprintFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        let fp = result[0].get("fingerprint").expect("fingerprint field");
        let hash_str = fp.as_str().expect("string");
        assert_eq!(hash_str.len(), 32); // MD5 hex = 32 chars
    }

    #[tokio::test]
    async fn test_fingerprint_deterministic() {
        let settings = serde_json::json!({
            "source": ["message"],
            "method": "SHA256"
        });
        let filter = FingerprintFilter::from_config(&settings, None).expect("config");
        let e1 = Event::new("same message");
        let e2 = Event::new("same message");
        let r1 = filter.filter(e1).await.expect("filter");
        let r2 = filter.filter(e2).await.expect("filter");
        assert_eq!(r1[0].get("fingerprint"), r2[0].get("fingerprint"));
    }

    #[tokio::test]
    async fn test_fingerprint_hmac() {
        let settings = serde_json::json!({
            "source": ["message"],
            "method": "SHA256",
            "key": "secret"
        });
        let filter = FingerprintFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].get("fingerprint").is_some());
    }

    #[tokio::test]
    async fn test_fingerprint_murmur3() {
        let settings = serde_json::json!({
            "source": ["message"],
            "method": "MURMUR3"
        });
        let filter = FingerprintFilter::from_config(&settings, None).expect("config");
        let event = Event::new("hello world");
        let result = filter.filter(event).await.expect("filter");
        assert!(result[0].get("fingerprint").is_some());
    }

    #[test]
    fn test_fingerprint_name() {
        let settings = serde_json::json!({});
        let filter = FingerprintFilter::from_config(&settings, None).expect("config");
        assert_eq!(filter.name(), "fingerprint");
    }
}
