// SPDX-License-Identifier: Apache-2.0
//! Email output — sends one email per event over SMTP via the `lettre` client
//! (rustls TLS stance). Mirrors Logstash's `email` output for the common case.
//!
//! ```logstash
//! output {
//!   email {
//!     address  => "smtp.example.com"      # SMTP host
//!     port     => 587
//!     username => "user"                  # optional → enables SMTP AUTH + STARTTLS
//!     password => "pass"
//!     to       => "ops@example.com"       # %{field}-aware
//!     from     => "logstash@ferro-stash"
//!     subject  => "alert: %{host}"        # %{field}-aware
//!     body     => "%{message}"            # %{field}-aware
//!     # htmlbody => "<b>%{message}</b>"   # %{field}-aware (alternative part)
//!   }
//! }
//! ```
//!
//! When `username`/`password` are set, the transport uses STARTTLS + SMTP AUTH;
//! otherwise it connects in plaintext (`builder_dangerous`). The transport (a
//! connection pool) is built once and reused; one message is sent per event.

use async_trait::async_trait;
use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::event::Event;
use ferro_stash_core::plugin::OutputPlugin;
use ferro_stash_core::settings_helpers::SettingsExt;
use lettre::message::header::ContentType;
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Address, AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

/// Email output configuration — mirrors the Logstash email output settings.
///
/// `Debug` is implemented manually so the SMTP `password` secret is never
/// rendered in logs/diagnostics (`{:?}` prints `"***"`, not the plaintext).
#[derive(Clone)]
pub struct EmailOutputConfig {
    pub address: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub to: String,
    pub from: String,
    pub subject: String,
    pub body: Option<String>,
    pub htmlbody: Option<String>,
}

impl std::fmt::Debug for EmailOutputConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailOutputConfig")
            .field("address", &self.address)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("to", &self.to)
            .field("from", &self.from)
            .field("subject", &self.subject)
            .field("body", &self.body)
            .field("htmlbody", &self.htmlbody)
            .finish()
    }
}

pub struct EmailOutput {
    config: EmailOutputConfig,
    condition: Option<Condition>,
    mailer: AsyncSmtpTransport<Tokio1Executor>,
}

impl std::fmt::Debug for EmailOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailOutput")
            .field("config", &self.config)
            .field("condition", &self.condition)
            .finish_non_exhaustive()
    }
}

/// Parse an address into a `lettre::Mailbox`, with a lenient fallback for a bare
/// `user@domain` whose domain is a single label (e.g. the default
/// `logstash@ferro-stash`), which the strict RFC parser rejects.
fn parse_mailbox(addr: &str) -> Result<Mailbox> {
    if let Ok(mb) = addr.parse::<Mailbox>() {
        return Ok(mb);
    }
    if let Some((user, domain)) = addr.rsplit_once('@') {
        if !user.is_empty()
            && !domain.is_empty()
            && !user.contains(['<', '>', ' '])
            && !domain.contains(['<', '>', ' '])
        {
            return Ok(Mailbox::new(None, Address::new_dangerous(user, domain)));
        }
    }
    Err(FerroStashError::Output {
        plugin: "email".to_string(),
        message: format!("invalid email address: {addr:?}"),
    })
}

impl EmailOutput {
    pub fn from_config(settings: &serde_json::Value, condition: Option<Condition>) -> Result<Self> {
        let err = |m: String| FerroStashError::Output {
            plugin: "email".to_string(),
            message: m,
        };
        let to = settings
            .get_string("to")
            .ok_or_else(|| err("email output requires `to`".to_string()))?;
        let port = settings
            .get_port("port", 25)
            .map_err(|message| FerroStashError::Output {
                plugin: "email".to_string(),
                message,
            })?;
        let address = settings
            .get_string("address")
            .unwrap_or_else(|| "localhost".to_string());
        let username = settings.get_string("username");
        let password = settings.get_string("password");

        // With credentials → STARTTLS relay + SMTP AUTH; otherwise plaintext.
        let mailer = match (&username, &password) {
            (Some(user), Some(pass)) => {
                AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&address)
                    .map_err(|e| err(format!("SMTP TLS setup failed: {e}")))?
                    .port(port)
                    .credentials(Credentials::new(user.clone(), pass.clone()))
                    .build()
            }
            _ => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(address.clone())
                .port(port)
                .build(),
        };

        Ok(Self {
            config: EmailOutputConfig {
                address,
                port,
                username,
                password,
                to,
                from: settings
                    .get_string("from")
                    .unwrap_or_else(|| "logstash@ferro-stash".to_string()),
                subject: settings.get_string("subject").unwrap_or_default(),
                body: settings.get_string("body"),
                htmlbody: settings.get_string("htmlbody"),
            },
            condition,
            mailer,
        })
    }

    /// Build the `lettre::Message` for one event (resolving `%{field}` templates).
    fn build_message(&self, event: &Event) -> Result<Message> {
        let to = parse_mailbox(&event.sprintf(&self.config.to))?;
        let from = parse_mailbox(&self.config.from)?;
        let subject = event.sprintf(&self.config.subject);

        let builder = Message::builder().from(from).to(to).subject(subject);

        let plain = self.config.body.as_ref().map(|b| event.sprintf(b));
        let html = self.config.htmlbody.as_ref().map(|b| event.sprintf(b));

        let map_err = |e: lettre::error::Error| FerroStashError::Output {
            plugin: "email".to_string(),
            message: format!("message build error: {e}"),
        };

        match (plain, html) {
            // Both → multipart/alternative.
            (Some(p), Some(h)) => builder
                .multipart(MultiPart::alternative_plain_html(p, h))
                .map_err(map_err),
            (None, Some(h)) => builder.singlepart(SinglePart::html(h)).map_err(map_err),
            (Some(p), None) => builder.singlepart(SinglePart::plain(p)).map_err(map_err),
            // Neither configured → default the event as text (JSON).
            (None, None) => builder
                .header(ContentType::TEXT_PLAIN)
                .body(event.to_json_string())
                .map_err(map_err),
        }
    }
}

#[async_trait]
impl OutputPlugin for EmailOutput {
    fn name(&self) -> &str {
        "email"
    }

    async fn output(&self, events: Vec<Event>) -> Result<()> {
        for event in &events {
            let message = self.build_message(event)?;
            self.mailer
                .send(message)
                .await
                .map_err(|e| FerroStashError::Output {
                    plugin: "email".to_string(),
                    message: format!("SMTP send failed: {e}"),
                })?;
        }
        Ok(())
    }

    fn condition(&self) -> Option<&Condition> {
        self.condition.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_to() {
        assert!(EmailOutput::from_config(&serde_json::json!({}), None).is_err());
        assert!(EmailOutput::from_config(&serde_json::json!({ "to": "x@y.com" }), None).is_ok());
    }

    #[test]
    fn defaults() {
        let o = EmailOutput::from_config(&serde_json::json!({ "to": "ops@example.com" }), None)
            .expect("config");
        assert_eq!(o.config.address, "localhost");
        assert_eq!(o.config.port, 25);
        assert_eq!(o.config.from, "logstash@ferro-stash");
        assert!(o.config.username.is_none());
        assert_eq!(o.name(), "email");
    }

    #[test]
    fn port_validation() {
        assert!(EmailOutput::from_config(
            &serde_json::json!({ "to": "x@y.com", "port": 70000 }),
            None
        )
        .is_err());
    }

    #[test]
    fn default_from_parses() {
        // The documented default `logstash@ferro-stash` (single-label domain) must
        // parse via the lenient fallback so the default config actually works.
        assert!(parse_mailbox("logstash@ferro-stash").is_ok());
        assert!(parse_mailbox("ops@example.com").is_ok());
        assert!(parse_mailbox("not-an-email").is_err());
    }

    #[test]
    fn debug_redacts_password() {
        let o = EmailOutput::from_config(
            &serde_json::json!({
                "to": "x@y.com", "username": "u", "password": "super-secret-pw",
                "address": "smtp.example.com"
            }),
            None,
        )
        .expect("config");
        let cfg_dbg = format!("{:?}", o.config);
        assert!(!cfg_dbg.contains("super-secret-pw"), "leaked: {cfg_dbg}");
        assert!(cfg_dbg.contains("***"));
        assert!(cfg_dbg.contains("smtp.example.com"));
        let out_dbg = format!("{o:?}");
        assert!(
            !out_dbg.contains("super-secret-pw"),
            "wrapper leaked: {out_dbg}"
        );
    }

    #[test]
    fn build_message_interpolates_subject_and_to() {
        let o = EmailOutput::from_config(
            &serde_json::json!({
                "to": "ops-%{team}@example.com",
                "subject": "alert: %{host}",
                "body": "msg=%{message}",
            }),
            None,
        )
        .expect("config");
        let mut ev = Event::new("disk full");
        ev.set(
            "host",
            ferro_stash_core::event::EventValue::String("h1".into()),
        );
        ev.set(
            "team",
            ferro_stash_core::event::EventValue::String("sre".into()),
        );
        // Building the message proves the recipient/subject/body resolved + parsed.
        let msg = o.build_message(&ev).expect("message builds");
        let formatted = String::from_utf8_lossy(&msg.formatted()).to_string();
        assert!(formatted.contains("ops-sre@example.com"), "to: {formatted}");
        assert!(formatted.contains("alert: h1"), "subject: {formatted}");
        assert!(formatted.contains("msg=disk full"), "body: {formatted}");
    }

    #[test]
    fn build_message_multipart_alternative() {
        let o = EmailOutput::from_config(
            &serde_json::json!({
                "to": "x@y.com", "body": "plain %{message}", "htmlbody": "<b>%{message}</b>"
            }),
            None,
        )
        .expect("config");
        let msg = o.build_message(&Event::new("hi")).expect("message builds");
        let formatted = String::from_utf8_lossy(&msg.formatted()).to_string();
        assert!(formatted.contains("multipart/alternative"), "{formatted}");
        assert!(formatted.contains("plain hi"));
        assert!(formatted.contains("<b>hi</b>"));
    }

    /// Live smoke (real SMTP): set `SMTP_HOST` (+ optional `SMTP_PORT`,
    /// `SMTP_USERNAME`/`SMTP_PASSWORD`, `SMTP_TO`, `SMTP_FROM`); sends one email.
    /// Run with a reachable SMTP server (e.g. MailHog on :1025):
    ///   SMTP_HOST=localhost SMTP_PORT=1025 SMTP_TO=ops@example.com \
    ///     cargo test -p ferro-stash-output -- --ignored email_live
    #[tokio::test]
    #[ignore = "live: set SMTP_HOST (reachable SMTP server)"]
    async fn email_live_sends() {
        let Ok(host) = std::env::var("SMTP_HOST") else {
            eprintln!("SKIPPED: set SMTP_HOST");
            return;
        };
        let mut cfg = serde_json::json!({
            "address": host,
            "port": std::env::var("SMTP_PORT").ok().and_then(|p| p.parse::<u64>().ok()).unwrap_or(25),
            "to": std::env::var("SMTP_TO").unwrap_or_else(|_| "ops@example.com".to_string()),
            "from": std::env::var("SMTP_FROM").unwrap_or_else(|_| "logstash@ferro-stash".to_string()),
            "subject": "ferro-stash email live smoke",
            "body": "%{message}",
        });
        if let (Ok(u), Ok(p)) = (
            std::env::var("SMTP_USERNAME"),
            std::env::var("SMTP_PASSWORD"),
        ) {
            cfg["username"] = serde_json::Value::String(u);
            cfg["password"] = serde_json::Value::String(p);
        }
        let output = EmailOutput::from_config(&cfg, None).expect("config");
        output
            .output(vec![Event::new("email live smoke")])
            .await
            .expect("live SMTP send should succeed");
    }
}
