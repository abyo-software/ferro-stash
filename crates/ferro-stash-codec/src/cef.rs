// SPDX-License-Identifier: Apache-2.0
//! CEF (Common Event Format) codec — ArcSight/SIEM standard event format.
//!
//! Format: `CEF:Version|Device Vendor|Device Product|Device Version|Signature ID|Name|Severity|Extension`
//!
//! Example: `CEF:0|Security|IDS|1.0|100|Attack detected|9|src=10.0.0.1 dst=192.168.1.1 msg=SQL injection`

use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::{Event, EventValue};

use crate::Codec;

/// Standard CEF key name mappings to full field names (ArcSight convention).
fn cef_key_full_name(key: &str) -> &str {
    match key {
        "act" => "deviceAction",
        "app" => "applicationProtocol",
        "c6a1" => "deviceCustomIPv6Address1",
        "c6a1Label" => "deviceCustomIPv6Address1Label",
        "cfp1" => "deviceCustomFloatingPoint1",
        "cfp1Label" => "deviceCustomFloatingPoint1Label",
        "cn1" => "deviceCustomNumber1",
        "cn1Label" => "deviceCustomNumber1Label",
        "cn2" => "deviceCustomNumber2",
        "cn2Label" => "deviceCustomNumber2Label",
        "cn3" => "deviceCustomNumber3",
        "cn3Label" => "deviceCustomNumber3Label",
        "cnt" => "baseEventCount",
        "cs1" => "deviceCustomString1",
        "cs1Label" => "deviceCustomString1Label",
        "cs2" => "deviceCustomString2",
        "cs2Label" => "deviceCustomString2Label",
        "cs3" => "deviceCustomString3",
        "cs3Label" => "deviceCustomString3Label",
        "cs4" => "deviceCustomString4",
        "cs4Label" => "deviceCustomString4Label",
        "cs5" => "deviceCustomString5",
        "cs5Label" => "deviceCustomString5Label",
        "cs6" => "deviceCustomString6",
        "cs6Label" => "deviceCustomString6Label",
        "dhost" => "destinationHostName",
        "dmac" => "destinationMacAddress",
        "dntdom" => "destinationNtDomain",
        "dpid" => "destinationProcessId",
        "dpriv" => "destinationUserPrivileges",
        "dproc" => "destinationProcessName",
        "dpt" => "destinationPort",
        "dst" => "destinationAddress",
        "duid" => "destinationUserId",
        "duser" => "destinationUserName",
        "dvc" => "deviceAddress",
        "dvchost" => "deviceHostName",
        "dvcpid" => "deviceProcessId",
        "end" => "endTime",
        "fname" => "fileName",
        "fsize" => "fileSize",
        "in" => "bytesIn",
        "msg" => "message",
        "out" => "bytesOut",
        "outcome" => "eventOutcome",
        "proto" => "transportProtocol",
        "request" => "requestUrl",
        "rt" => "deviceReceiptTime",
        "shost" => "sourceHostName",
        "smac" => "sourceMacAddress",
        "sntdom" => "sourceNtDomain",
        "spid" => "sourceProcessId",
        "spriv" => "sourceUserPrivileges",
        "sproc" => "sourceProcessName",
        "spt" => "sourcePort",
        "src" => "sourceAddress",
        "start" => "startTime",
        "suid" => "sourceUserId",
        "suser" => "sourceUserName",
        "type" => "categoryDeviceType",
        _ => key,
    }
}

/// CEF (Common Event Format) codec.
#[derive(Debug, Clone, Default)]
pub struct CefCodec {
    /// Default severity for encoding (0-10).
    pub default_severity: Option<String>,
}

impl CefCodec {
    pub fn from_config(settings: &serde_json::Value) -> Result<Self> {
        let default_severity = settings
            .get("default_severity")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self { default_severity })
    }

    /// Parse a CEF extension string into key-value pairs.
    /// Extension format: `key1=value1 key2=value2`
    /// Values can contain spaces — a new key starts at `\w+=`.
    fn parse_extensions(ext_str: &str) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        let mut current_key = String::new();
        let mut current_value = String::new();
        let mut in_value = false;

        for token in ext_str.split('=') {
            if !in_value {
                current_key = token.trim().to_string();
                in_value = true;
            } else {
                // The token might contain "value nextkey" — split on last space
                if let Some(space_pos) = token.rfind(' ') {
                    let value_part = &token[..space_pos];
                    let next_key = &token[space_pos + 1..];

                    if !current_value.is_empty() {
                        current_value.push('=');
                    }
                    current_value.push_str(value_part);

                    if !current_key.is_empty() {
                        pairs.push((current_key.clone(), current_value.trim().to_string()));
                    }
                    current_key = next_key.to_string();
                    current_value = String::new();
                } else {
                    if !current_value.is_empty() {
                        current_value.push('=');
                    }
                    current_value.push_str(token);
                }
            }
        }

        // Push the last pair
        if !current_key.is_empty() {
            pairs.push((current_key, current_value.trim().to_string()));
        }

        pairs
    }

    /// Unescape CEF pipe-delimited field (pipes escaped as `\|`).
    fn unescape_field(s: &str) -> String {
        s.replace("\\|", "|").replace("\\\\", "\\")
    }
}

impl Codec for CefCodec {
    fn name(&self) -> &'static str {
        "cef"
    }

    fn decode(&self, data: &[u8]) -> Result<Vec<Event>> {
        let text = String::from_utf8_lossy(data);
        let text = text.trim();

        // Strip optional syslog header (anything before "CEF:")
        let cef_start = text.find("CEF:").ok_or_else(|| {
            FerroStashError::Codec("not a CEF message: missing CEF: prefix".to_string())
        })?;

        let syslog_header = if cef_start > 0 {
            Some(text[..cef_start].trim().to_string())
        } else {
            None
        };

        let cef_part = &text[cef_start + 4..]; // skip "CEF:"

        // Split on unescaped pipes — we need exactly 7 pipe-separated fields
        let mut fields = Vec::new();
        let mut current = String::new();
        let mut chars = cef_part.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    current.push(ch);
                    current.push(next);
                    chars.next();
                    continue;
                }
            }
            if ch == '|' && fields.len() < 7 {
                fields.push(current.clone());
                current.clear();
            } else {
                current.push(ch);
            }
        }
        // The remainder is the extension field
        let extension_str = current;

        if fields.len() < 7 {
            return Err(FerroStashError::Codec(format!(
                "invalid CEF format: expected 7 pipe-delimited header fields, got {}",
                fields.len()
            )));
        }

        let mut event = Event::empty();
        event.set(
            "cef_version",
            EventValue::String(Self::unescape_field(&fields[0])),
        );
        event.set(
            "device_vendor",
            EventValue::String(Self::unescape_field(&fields[1])),
        );
        event.set(
            "device_product",
            EventValue::String(Self::unescape_field(&fields[2])),
        );
        event.set(
            "device_version",
            EventValue::String(Self::unescape_field(&fields[3])),
        );
        event.set(
            "signature_id",
            EventValue::String(Self::unescape_field(&fields[4])),
        );
        event.set("name", EventValue::String(Self::unescape_field(&fields[5])));
        event.set(
            "severity",
            EventValue::String(Self::unescape_field(&fields[6])),
        );

        if let Some(header) = syslog_header {
            event.set("syslog_header", EventValue::String(header));
        }

        // Parse extensions
        if !extension_str.is_empty() {
            for (key, value) in Self::parse_extensions(&extension_str) {
                let full_name = cef_key_full_name(&key);
                event.set(full_name.to_string(), EventValue::String(value));
            }
        }

        // Set message
        event.set_message(text);

        Ok(vec![event])
    }

    fn encode(&self, event: &Event) -> Result<Vec<u8>> {
        let version = event
            .get("cef_version")
            .and_then(EventValue::as_str)
            .unwrap_or("0");
        let vendor = event
            .get("device_vendor")
            .and_then(EventValue::as_str)
            .unwrap_or("FerroStash");
        let product = event
            .get("device_product")
            .and_then(EventValue::as_str)
            .unwrap_or("FerroStash");
        let dev_version = event
            .get("device_version")
            .and_then(EventValue::as_str)
            .unwrap_or("1.0");
        let sig_id = event
            .get("signature_id")
            .and_then(EventValue::as_str)
            .unwrap_or("0");
        let name = event.get("name").and_then(EventValue::as_str).unwrap_or("");
        let severity = event
            .get("severity")
            .and_then(EventValue::as_str)
            .or(self.default_severity.as_deref())
            .unwrap_or("5");

        // Build extension string from remaining fields
        let reserved = [
            "cef_version",
            "device_vendor",
            "device_product",
            "device_version",
            "signature_id",
            "name",
            "severity",
            "message",
            "syslog_header",
        ];

        let mut extensions = Vec::new();
        for (k, v) in event.fields() {
            if !reserved.contains(&k.as_str()) {
                extensions.push(format!("{}={}", k, v.to_string_lossy()));
            }
        }

        let ext_str = extensions.join(" ");
        let line = format!(
            "CEF:{version}|{vendor}|{product}|{dev_version}|{sig_id}|{name}|{severity}|{ext_str}\n"
        );
        Ok(line.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cef_decode() {
        let codec = CefCodec::default();
        let data = b"CEF:0|Security|IDS|1.0|100|Attack|9|src=10.0.0.1 dst=192.168.1.1";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("device_vendor"),
            Some(&EventValue::String("Security".into()))
        );
        assert_eq!(
            event.get("device_product"),
            Some(&EventValue::String("IDS".into()))
        );
        assert_eq!(event.get("severity"), Some(&EventValue::String("9".into())));
        assert_eq!(
            event.get("sourceAddress"),
            Some(&EventValue::String("10.0.0.1".into()))
        );
        assert_eq!(
            event.get("destinationAddress"),
            Some(&EventValue::String("192.168.1.1".into()))
        );
    }

    #[test]
    fn test_cef_decode_with_syslog_header() {
        let codec = CefCodec::default();
        let data = b"Apr 12 10:00:00 host CEF:0|Vendor|Product|1.0|1|Test|5|msg=hello";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert!(event.has_field("syslog_header"));
        assert_eq!(
            event.get("device_vendor"),
            Some(&EventValue::String("Vendor".into()))
        );
    }

    #[test]
    fn test_cef_encode() {
        let codec = CefCodec::default();
        let mut event = Event::empty();
        event.set("cef_version", EventValue::String("0".into()));
        event.set("device_vendor", EventValue::String("Test".into()));
        event.set("device_product", EventValue::String("App".into()));
        event.set("device_version", EventValue::String("1.0".into()));
        event.set("signature_id", EventValue::String("100".into()));
        event.set("name", EventValue::String("Event".into()));
        event.set("severity", EventValue::String("5".into()));
        event.set("src", EventValue::String("10.0.0.1".into()));
        let bytes = codec.encode(&event).expect("encode");
        let text = String::from_utf8(bytes).expect("utf8");
        assert!(text.starts_with("CEF:0|Test|App|1.0|100|Event|5|"));
        assert!(text.contains("src=10.0.0.1"));
    }

    #[test]
    fn test_cef_invalid() {
        let codec = CefCodec::default();
        assert!(codec.decode(b"not a CEF message").is_err());
        assert!(codec.decode(b"CEF:0|only|three|pipes").is_err());
    }

    #[test]
    fn test_cef_name() {
        assert_eq!(CefCodec::default().name(), "cef");
    }

    #[test]
    fn test_cef_escaped_pipe() {
        let codec = CefCodec::default();
        let data = b"CEF:0|Ven\\|dor|Product|1.0|1|Name|5|";
        let event = codec
            .decode(data)
            .expect("decode")
            .into_iter()
            .next()
            .expect("no events");
        assert_eq!(
            event.get("device_vendor"),
            Some(&EventValue::String("Ven|dor".into()))
        );
    }
}
