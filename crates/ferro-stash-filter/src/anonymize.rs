// SPDX-License-Identifier: Apache-2.0
//! Anonymize filter — replaces listed field values with a consistent hash.
//!
//! ```logstash
//! filter {
//!   anonymize {
//!     fields    => [ "ip", "user" ]
//!     algorithm => "SHA256"
//!     key       => "secret"      # optional → HMAC; absent → plain digest
//!   }
//! }
//! ```
//!
//! Supported algorithms: `SHA1`, `SHA256`, `SHA384`, `SHA512`, `MD5`, `MURMUR3`.
//! When `key` is present a keyed HMAC is used (for the SHA / MD5 family); without
//! a key a plain digest is used. `MURMUR3` is an unkeyed 32-bit hash (the
//! `MurmurHash3 x86_32` variant) and produces an integer; any `key` is ignored
//! for `MURMUR3` (documented residual — murmur3 is not a keyed MAC).

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};
use ferro_stash_core::plugin::FilterPlugin;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Algorithm {
    Sha1,
    Sha256,
    Sha384,
    Sha512,
    Md5,
    Murmur3,
}

#[derive(Debug)]
pub struct AnonymizeFilter {
    fields: Vec<String>,
    algorithm: Algorithm,
    key: Option<String>,
    condition: Option<Condition>,
}

impl AnonymizeFilter {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let err = |m: String| FerroStashError::Filter {
            plugin: "anonymize".to_string(),
            message: m,
        };

        let fields: Vec<String> = settings
            .get("fields")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        if fields.is_empty() {
            return Err(err(
                "anonymize filter requires a non-empty `fields` array".to_string(),
            ));
        }

        let algorithm = match settings
            .get("algorithm")
            .and_then(|v| v.as_str())
            .unwrap_or("SHA1")
            .to_uppercase()
            .as_str()
        {
            "SHA1" => Algorithm::Sha1,
            "SHA256" => Algorithm::Sha256,
            "SHA384" => Algorithm::Sha384,
            "SHA512" => Algorithm::Sha512,
            "MD5" => Algorithm::Md5,
            "MURMUR3" => Algorithm::Murmur3,
            other => {
                return Err(err(format!(
                    "unknown algorithm '{other}' (supported: SHA1, SHA256, SHA384, SHA512, MD5, MURMUR3)"
                )));
            }
        };

        let key = settings
            .get("key")
            .and_then(|v| v.as_str())
            .map(String::from);

        Ok(Self {
            fields,
            algorithm,
            key,
            condition,
        })
    }

    /// Computes the anonymized value for `data`. SHA/MD5 produce a hex string;
    /// MURMUR3 produces an integer (matching Logstash).
    fn anonymize(&self, data: &[u8]) -> EventValue {
        use sha2::Digest;

        // MURMUR3 is unkeyed; the key (if any) is ignored.
        if self.algorithm == Algorithm::Murmur3 {
            let h = murmur3_x86_32(data, 0);
            return EventValue::Integer(i64::from(h));
        }

        if let Some(key) = &self.key {
            use hmac::Mac;
            let hex = match self.algorithm {
                Algorithm::Sha1 => {
                    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                Algorithm::Sha256 => {
                    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                Algorithm::Sha384 => {
                    let mut mac = hmac::Hmac::<sha2::Sha384>::new_from_slice(key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                Algorithm::Sha512 => {
                    let mut mac = hmac::Hmac::<sha2::Sha512>::new_from_slice(key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                Algorithm::Md5 => {
                    let mut mac = hmac::Hmac::<md5::Md5>::new_from_slice(key.as_bytes())
                        .expect("HMAC accepts any key length");
                    mac.update(data);
                    hex::encode(mac.finalize().into_bytes())
                }
                Algorithm::Murmur3 => unreachable!("murmur3 handled above"),
            };
            return EventValue::String(hex);
        }

        let hex = match self.algorithm {
            Algorithm::Sha1 => hex::encode(sha1::Sha1::digest(data)),
            Algorithm::Sha256 => hex::encode(sha2::Sha256::digest(data)),
            Algorithm::Sha384 => hex::encode(sha2::Sha384::digest(data)),
            Algorithm::Sha512 => hex::encode(sha2::Sha512::digest(data)),
            Algorithm::Md5 => hex::encode(md5::Md5::digest(data)),
            Algorithm::Murmur3 => unreachable!("murmur3 handled above"),
        };
        EventValue::String(hex)
    }
}

/// `MurmurHash3 x86_32` — the 32-bit variant used by Logstash's anonymize
/// filter. Deterministic for a given input and seed.
fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    let mut h = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13);
        h = h.wrapping_mul(5).wrapping_add(0xe654_6b64);
    }

    let tail = chunks.remainder();
    if !tail.is_empty() {
        let mut k: u32 = 0;
        if tail.len() >= 3 {
            k ^= u32::from(tail[2]) << 16;
        }
        if tail.len() >= 2 {
            k ^= u32::from(tail[1]) << 8;
        }
        k ^= u32::from(tail[0]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        h ^= k;
    }

    // Finalization mix (fmix32).
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

#[async_trait]
impl FilterPlugin for AnonymizeFilter {
    fn name(&self) -> &'static str {
        "anonymize"
    }

    async fn filter(&self, mut event: Event) -> Result<Vec<Event>> {
        for field in &self.fields {
            let Some(value) = event.get(field) else {
                continue;
            };
            let data = value.to_string_lossy();
            let hashed = self.anonymize(data.as_bytes());
            event.set(field.clone(), hashed);
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

    fn mk(settings: serde_json::Value) -> AnonymizeFilter {
        AnonymizeFilter::from_config(&settings, None).expect("config")
    }

    #[test]
    fn test_anonymize_name() {
        let f = mk(serde_json::json!({ "fields": ["a"] }));
        assert_eq!(f.name(), "anonymize");
    }

    #[test]
    fn test_anonymize_requires_fields() {
        assert!(AnonymizeFilter::from_config(&serde_json::json!({}), None).is_err());
        assert!(AnonymizeFilter::from_config(&serde_json::json!({ "fields": [] }), None).is_err());
    }

    #[test]
    fn test_anonymize_unknown_algorithm_errors() {
        let err = AnonymizeFilter::from_config(
            &serde_json::json!({ "fields": ["a"], "algorithm": "BOGUS" }),
            None,
        );
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_anonymize_sha1_default() {
        let f = mk(serde_json::json!({ "fields": ["ip"] }));
        let mut event = Event::new("x");
        event.set("ip", EventValue::String("1.2.3.4".into()));
        let out = f.filter(event).await.expect("filter");
        let s = out[0].get("ip").expect("ip").as_str().expect("str");
        // SHA1 hex digest is 40 chars.
        assert_eq!(s.len(), 40);
        // Known SHA1("1.2.3.4").
        assert_eq!(s, "09c35807ba47a82592ef88e5d6304ea699b8cbe2");
    }

    #[tokio::test]
    async fn test_anonymize_sha256_lengths() {
        let f = mk(serde_json::json!({ "fields": ["v"], "algorithm": "SHA256" }));
        let mut event = Event::new("x");
        event.set("v", EventValue::String("hello".into()));
        let out = f.filter(event).await.expect("filter");
        assert_eq!(out[0].get("v").expect("v").as_str().expect("s").len(), 64);
    }

    #[tokio::test]
    async fn test_anonymize_sha384_sha512_md5_lengths() {
        for (algo, len) in [("SHA384", 96), ("SHA512", 128), ("MD5", 32)] {
            let f = mk(serde_json::json!({ "fields": ["v"], "algorithm": algo }));
            let mut event = Event::new("x");
            event.set("v", EventValue::String("hello".into()));
            let out = f.filter(event).await.expect("filter");
            assert_eq!(
                out[0].get("v").expect("v").as_str().expect("s").len(),
                len,
                "algorithm {algo}"
            );
        }
    }

    #[tokio::test]
    async fn test_anonymize_stable_hash() {
        let f = mk(serde_json::json!({ "fields": ["v"], "algorithm": "SHA256" }));
        let mut e1 = Event::new("x");
        e1.set("v", EventValue::String("same".into()));
        let mut e2 = Event::new("y");
        e2.set("v", EventValue::String("same".into()));
        let r1 = f.filter(e1).await.expect("filter");
        let r2 = f.filter(e2).await.expect("filter");
        assert_eq!(r1[0].get("v"), r2[0].get("v"));
    }

    #[tokio::test]
    async fn test_anonymize_hmac_differs_from_plain() {
        let plain = mk(serde_json::json!({ "fields": ["v"], "algorithm": "SHA256" }));
        let keyed = mk(serde_json::json!({ "fields": ["v"], "algorithm": "SHA256", "key": "secret" }));
        let mut e1 = Event::new("x");
        e1.set("v", EventValue::String("data".into()));
        let mut e2 = Event::new("x");
        e2.set("v", EventValue::String("data".into()));
        let r_plain = plain.filter(e1).await.expect("filter");
        let r_keyed = keyed.filter(e2).await.expect("filter");
        assert_ne!(r_plain[0].get("v"), r_keyed[0].get("v"));
        // HMAC-SHA256 hex is still 64 chars.
        assert_eq!(r_keyed[0].get("v").expect("v").as_str().expect("s").len(), 64);
    }

    #[tokio::test]
    async fn test_anonymize_murmur3_integer_and_stable() {
        let f = mk(serde_json::json!({ "fields": ["v"], "algorithm": "MURMUR3" }));
        let mut e1 = Event::new("x");
        e1.set("v", EventValue::String("hello".into()));
        let mut e2 = Event::new("y");
        e2.set("v", EventValue::String("hello".into()));
        let r1 = f.filter(e1).await.expect("filter");
        let r2 = f.filter(e2).await.expect("filter");
        // MURMUR3 yields an integer and is deterministic.
        assert!(matches!(r1[0].get("v"), Some(EventValue::Integer(_))));
        assert_eq!(r1[0].get("v"), r2[0].get("v"));
    }

    #[test]
    fn test_murmur3_known_vectors() {
        // Reference MurmurHash3 x86_32 vectors (seed 0). `test` matches the
        // canonical published vector (0xba6bd213), validating the encoding.
        assert_eq!(murmur3_x86_32(b"", 0), 0);
        assert_eq!(murmur3_x86_32(b"test", 0), 0xba6b_d213);
        assert_eq!(murmur3_x86_32(b"hello", 0), 0x248b_fa47);
        assert_eq!(murmur3_x86_32(b"The quick brown fox", 0), 0x60a2_c22d);
    }

    #[tokio::test]
    async fn test_anonymize_missing_field_skipped() {
        let f = mk(serde_json::json!({ "fields": ["absent"], "algorithm": "SHA256" }));
        let out = f.filter(Event::new("x")).await.expect("filter");
        assert!(out[0].get("absent").is_none());
    }
}
