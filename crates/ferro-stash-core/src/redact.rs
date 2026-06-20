// SPDX-License-Identifier: Apache-2.0
//! Shared log-safe redaction helpers.
//!
//! Secrets (credentials, tokens, signing keys) routinely hide inside URLs
//! (userinfo, signed query params) and inside the free-form `settings`
//! `serde_json::Value` blobs that configure every plugin. A derived `Debug`
//! or a naive `tracing` field would spill them into logs verbatim.
//!
//! These helpers produce a redacted *rendering* only — they never mutate the
//! real values used at runtime, so config round-trips (Serialize/Deserialize)
//! are unaffected. Both are best-effort and must never panic.

/// Query-parameter names (matched case-insensitively, exact) whose VALUE is a
/// secret and must be masked in a log-safe URL rendering.
const SENSITIVE_QUERY_PARAMS: &[&str] = &[
    "token",
    "access_token",
    "api_key",
    "apikey",
    "key",
    "sig",
    "signature",
    "password",
    "secret",
];

/// JSON object key substrings (matched case-insensitively) whose VALUE is a
/// secret and must be masked. A key is sensitive when its lowercased form
/// CONTAINS any of these (so `api_key`, `db_password`, `x-webhook-url` all
/// match). `key` is intentionally a substring match per the redaction spec.
const SENSITIVE_JSON_KEY_SUBSTR: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "credential",
    "passphrase",
    "private_key",
    "access_key",
    "session_token",
    "sasl",
    "webhook",
    "key",
];

/// Strip the `user:pass@` userinfo from a URL's authority (best-effort).
///
/// Handles both `scheme://userinfo@host/path` and a schemeless
/// `userinfo@host/path`. The authority ends at the first `/`; any `@` within it
/// is treated as the userinfo separator (last `@` wins). Never panics — all
/// splits are on ASCII delimiters located via `find`/`rfind`.
fn strip_userinfo(base: &str) -> String {
    if let Some(scheme_end) = base.find("://") {
        // Keep the `scheme://` prefix, work on the part after it.
        let (scheme, after) = base.split_at(scheme_end + 3);
        let auth_end = after.find('/').unwrap_or(after.len());
        let authority = &after[..auth_end];
        let path = &after[auth_end..];
        if let Some(at) = authority.rfind('@') {
            return format!("{scheme}{host}{path}", host = &authority[at + 1..]);
        }
        base.to_string()
    } else {
        // No scheme: best-effort strip of a leading `userinfo@` in the part
        // before the first `/`.
        let auth_end = base.find('/').unwrap_or(base.len());
        let authority = &base[..auth_end];
        if let Some(at) = authority.rfind('@') {
            return format!("{}{}", &authority[at + 1..], &base[auth_end..]);
        }
        base.to_string()
    }
}

/// Mask the values of known-sensitive params in a `k=v&k2=v2` query string,
/// leaving non-secret params (and the param names) intact.
fn redact_query(query: &str) -> String {
    query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((k, _)) if SENSITIVE_QUERY_PARAMS.contains(&k.to_ascii_lowercase().as_str()) => {
                format!("{k}=***")
            }
            _ => pair.to_string(),
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Return a log-safe rendering of `url`: strip URL userinfo
/// (`user:pass@host` → `host`) and mask the values of known-sensitive query
/// params (`token`, `access_token`, `api_key`, `apikey`, `key`, `sig`,
/// `signature`, `password`, `secret`) with `***`.
///
/// Scheme, host, path, and non-secret query/fragment params stay visible. The
/// input need not be a well-formed URL — parsing is best-effort and never
/// panics; a non-URL string is returned unchanged except that an
/// `userinfo@` prefix is still stripped if present.
#[must_use]
pub fn redact_url(url: &str) -> String {
    // Peel off the fragment, then the query. Both can carry a secret (e.g.
    // `#access_token=…`), so redact the fragment's k=v pairs too.
    let (rest, fragment) = match url.find('#') {
        Some(i) => (&url[..i], &url[i + 1..]),
        None => (url, ""),
    };
    let (base, query) = match rest.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (rest, None),
    };
    let base = strip_userinfo(base);
    let frag = if fragment.is_empty() {
        String::new()
    } else {
        format!("#{}", redact_query(fragment))
    };
    match query {
        Some(q) => format!("{base}?{}{frag}", redact_query(q)),
        None => format!("{base}{frag}"),
    }
}

/// Deep-clone `v`, replacing the VALUE of every object entry whose key
/// (lowercased) contains a known-sensitive substring with `"***"` (regardless
/// of the original value's type). Non-secret object entries, array elements,
/// and scalars are recursed into / passed through unchanged.
#[must_use]
pub fn redact_secrets_in_json(v: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                let lc = k.to_ascii_lowercase();
                if SENSITIVE_JSON_KEY_SUBSTR.iter().any(|s| lc.contains(*s)) {
                    out.insert(k.clone(), Value::String("***".to_string()));
                } else {
                    out.insert(k.clone(), redact_secrets_in_json(val));
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(redact_secrets_in_json).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_strips_userinfo() {
        let out = redact_url("https://user:secretpw@example.com/path");
        assert!(!out.contains("secretpw"), "password leaked: {out}");
        assert!(!out.contains("user:"), "userinfo leaked: {out}");
        assert_eq!(out, "https://example.com/path");
    }

    #[test]
    fn redact_url_masks_secret_query_param() {
        let out = redact_url("https://example.com/?token=abc123&page=2");
        assert!(!out.contains("abc123"), "token value leaked: {out}");
        assert!(out.contains("token=***"), "token not masked: {out}");
    }

    #[test]
    fn redact_url_keeps_non_secret_query_param() {
        let out = redact_url("https://example.com/?token=abc123&page=2");
        // Non-secret params and the host/path stay visible.
        assert!(out.contains("page=2"), "non-secret param dropped: {out}");
        assert!(out.contains("example.com"), "host dropped: {out}");
    }

    #[test]
    fn redact_url_masks_signature_and_keeps_fragment() {
        let out = redact_url("http://h/p?sig=DEADBEEF&x=1#frag");
        assert!(!out.contains("DEADBEEF"), "signature leaked: {out}");
        assert!(out.contains("sig=***"));
        assert!(out.contains("x=1"));
        assert!(out.contains("#frag"), "fragment dropped: {out}");
    }

    #[test]
    fn redact_url_masks_secret_in_fragment() {
        let out = redact_url("https://api.example/p#access_token=secret&page=2");
        assert!(!out.contains("secret"), "fragment secret leaked: {out}");
        assert!(
            out.contains("access_token=***"),
            "fragment not masked: {out}"
        );
        assert!(
            out.contains("page=2"),
            "non-secret fragment param dropped: {out}"
        );
        // A plain (non k=v) fragment is still preserved.
        assert!(redact_url("http://h/p#section").contains("#section"));
    }

    #[test]
    fn redact_url_non_url_input_does_not_panic() {
        // A plain string with no URL structure is returned unchanged.
        assert_eq!(redact_url("just-a-string"), "just-a-string");
        // A schemeless userinfo@ is still stripped, best-effort.
        assert_eq!(redact_url("user:pass@host.com/x"), "host.com/x");
        // Pathological input must not panic.
        let _ = redact_url("not a url ::: @@@ ???");
        let _ = redact_url("");
    }

    #[test]
    fn redact_json_masks_nested_secret_keys() {
        let v = serde_json::json!({
            "host": "db.example.com",
            "creds": { "password": "hunter2", "username": "admin" },
            "api_key": "AKIA123",
            "nested": { "deeper": { "session_token": "xyz" } }
        });
        let r = redact_secrets_in_json(&v);
        // Nested secret key masked regardless of depth.
        assert_eq!(r["creds"]["password"], "***");
        assert_eq!(r["nested"]["deeper"]["session_token"], "***");
        assert_eq!(r["api_key"], "***");
        // Non-secret keys preserved (scalars and nested).
        assert_eq!(r["host"], "db.example.com");
        assert_eq!(r["creds"]["username"], "admin");
    }

    #[test]
    fn redact_json_masks_non_string_secret_values() {
        // A secret-keyed entry is masked even when the value is not a string.
        let v = serde_json::json!({ "secret": 12345, "tokens": [1, 2, 3] });
        let r = redact_secrets_in_json(&v);
        assert_eq!(r["secret"], "***");
        // "tokens" contains "token" → masked wholesale (value type ignored).
        assert_eq!(r["tokens"], "***");
    }

    #[test]
    fn redact_json_preserves_arrays_and_scalars() {
        let v = serde_json::json!({
            "items": [{ "name": "a", "password": "p" }, { "name": "b" }],
            "count": 7
        });
        let r = redact_secrets_in_json(&v);
        assert_eq!(r["items"][0]["password"], "***");
        assert_eq!(r["items"][0]["name"], "a");
        assert_eq!(r["items"][1]["name"], "b");
        assert_eq!(r["count"], 7);
    }
}
