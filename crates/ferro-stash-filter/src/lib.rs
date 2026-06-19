// SPDX-License-Identifier: Apache-2.0
//! Filter plugins for `FerroStash`.

pub mod aggregate;
pub mod anonymize;
pub mod bytes_filter;
pub mod cidr;
pub mod clone_filter;
pub mod csv_filter;
pub mod date;
pub mod de_dot;
pub mod dissect;
pub mod dns;
pub mod drop;
pub mod elasticsearch_filter;
pub mod fingerprint;
pub mod geoip;
pub mod grok;
pub mod json_encode;
pub mod json_filter;
pub mod kv;
pub mod metrics;
pub mod mutate;
pub mod prune;
#[cfg(feature = "ruby")]
pub mod ruby;
pub mod script;
pub mod sleep_filter;
pub mod split_filter;
pub mod syslog_pri;
pub mod throttle;
pub mod translate;
pub mod truncate;
pub mod urldecode;
pub mod useragent;
pub mod uuid_filter;
pub mod xml;

use ferro_stash_core::condition::Condition;
use ferro_stash_core::error::{FerroStashError, Result};
use ferro_stash_core::plugin::FilterPlugin;

/// Creates a filter plugin by name.
pub fn create_filter(
    name: &str,
    settings: &serde_json::Value,
    condition: Option<Condition>,
) -> Result<Box<dyn FilterPlugin>> {
    let filter: Box<dyn FilterPlugin> = match name {
        "grok" => Box::new(grok::GrokFilter::from_config(settings, condition)?),
        "mutate" => Box::new(mutate::MutateFilter::from_config(settings, condition)?),
        "json" => Box::new(json_filter::JsonFilter::from_config(settings, condition)?),
        "date" => Box::new(date::DateFilter::from_config(settings, condition)?),
        "dissect" => Box::new(dissect::DissectFilter::from_config(settings, condition)?),
        "kv" => Box::new(kv::KvFilter::from_config(settings, condition)?),
        "drop" => Box::new(drop::DropFilter::from_config(settings, condition)?),
        "clone" => Box::new(clone_filter::CloneFilter::from_config(settings, condition)?),
        #[cfg(feature = "ruby")]
        "ruby" => Box::new(ruby::RubyFilter::from_config(settings, condition)?),
        #[cfg(not(feature = "ruby"))]
        "ruby" => {
            return Err(FerroStashError::Filter {
                plugin: name.to_string(),
                message: "the `ruby` filter requires building with the `ruby` cargo \
                          feature (it embeds the Artichoke mruby interpreter); rebuild \
                          with `--features ruby`"
                    .to_string(),
            });
        }
        "script" | "painless" => Box::new(script::ScriptFilter::from_config(settings, condition)?),
        "geoip" => Box::new(geoip::GeoipFilter::from_config(settings, condition)?),
        "sleep" => Box::new(sleep_filter::SleepFilter::from_config(settings, condition)?),
        "aggregate" => Box::new(aggregate::AggregateFilter::from_config(
            settings, condition,
        )?),
        "throttle" => Box::new(throttle::ThrottleFilter::from_config(settings, condition)?),
        "translate" => Box::new(translate::TranslateFilter::from_config(
            settings, condition,
        )?),
        "fingerprint" => Box::new(fingerprint::FingerprintFilter::from_config(
            settings, condition,
        )?),
        "useragent" => Box::new(useragent::UseragentFilter::from_config(
            settings, condition,
        )?),
        "csv" => Box::new(csv_filter::CsvFilter::from_config(settings, condition)?),
        "urldecode" => Box::new(urldecode::UrldecodeFilter::from_config(
            settings, condition,
        )?),
        "split" => Box::new(split_filter::SplitFilter::from_config(settings, condition)?),
        "truncate" => Box::new(truncate::TruncateFilter::from_config(settings, condition)?),
        "prune" => Box::new(prune::PruneFilter::from_config(settings, condition)?),
        "xml" => Box::new(xml::XmlFilter::from_config(settings, condition)?),
        "dns" => Box::new(dns::DnsFilter::from_config(settings, condition)?),
        "elasticsearch" => Box::new(elasticsearch_filter::ElasticsearchFilter::from_config(
            settings, condition,
        )?),
        "metrics" => Box::new(metrics::MetricsFilter::from_config(settings, condition)?),
        "de_dot" => Box::new(de_dot::DeDotFilter::from_config(settings, condition)?),
        "json_encode" => Box::new(json_encode::JsonEncodeFilter::from_config(
            settings, condition,
        )?),
        "bytes" => Box::new(bytes_filter::BytesFilter::from_config(settings, condition)?),
        "cidr" => Box::new(cidr::CidrFilter::from_config(settings, condition)?),
        "uuid" => Box::new(uuid_filter::UuidFilter::from_config(settings, condition)?),
        "syslog_pri" => Box::new(syslog_pri::SyslogPriFilter::from_config(
            settings, condition,
        )?),
        "anonymize" => Box::new(anonymize::AnonymizeFilter::from_config(
            settings, condition,
        )?),
        _ => {
            return Err(FerroStashError::Filter {
                plugin: name.to_string(),
                message: format!("unknown filter plugin: {name}"),
            });
        }
    };
    Ok(filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_grok_filter() {
        let settings = serde_json::json!({ "match": { "message": "%{WORD:w}" } });
        let filter = create_filter("grok", &settings, None);
        assert!(filter.is_ok());
        assert_eq!(filter.expect("grok filter").name(), "grok");
    }

    #[test]
    fn test_create_mutate_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("mutate", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_json_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("json", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_date_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("date", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_dissect_filter() {
        let settings = serde_json::json!({ "mapping": "%{a} %{b}" });
        let filter = create_filter("dissect", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_kv_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("kv", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_drop_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("drop", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_clone_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("clone", &settings, None);
        assert!(filter.is_ok());
    }

    #[cfg(feature = "ruby")]
    #[test]
    fn test_create_ruby_filter() {
        let settings = serde_json::json!({ "code": "" });
        let filter = create_filter("ruby", &settings, None);
        assert!(filter.is_ok());
    }

    #[cfg(not(feature = "ruby"))]
    #[test]
    fn test_ruby_filter_errors_without_feature() {
        let settings = serde_json::json!({ "code": "" });
        let err = create_filter("ruby", &settings, None);
        assert!(
            err.is_err(),
            "ruby must error when built without the feature"
        );
    }

    #[test]
    fn test_create_geoip_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("geoip", &settings, None);
        assert!(filter.is_ok());
    }

    #[test]
    fn test_create_cidr_filter() {
        let settings = serde_json::json!({ "network": ["10.0.0.0/8"] });
        let filter = create_filter("cidr", &settings, None);
        assert!(filter.is_ok());
        assert_eq!(filter.expect("cidr filter").name(), "cidr");
    }

    #[test]
    fn test_create_uuid_filter() {
        let settings = serde_json::json!({ "target": "uuid" });
        let filter = create_filter("uuid", &settings, None);
        assert!(filter.is_ok());
        assert_eq!(filter.expect("uuid filter").name(), "uuid");
    }

    #[test]
    fn test_create_syslog_pri_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("syslog_pri", &settings, None);
        assert!(filter.is_ok());
        assert_eq!(filter.expect("syslog_pri filter").name(), "syslog_pri");
    }

    #[test]
    fn test_create_anonymize_filter() {
        let settings = serde_json::json!({ "fields": ["message"] });
        let filter = create_filter("anonymize", &settings, None);
        assert!(filter.is_ok());
        assert_eq!(filter.expect("anonymize filter").name(), "anonymize");
    }

    #[test]
    fn test_create_unknown_filter() {
        let settings = serde_json::json!({});
        let filter = create_filter("nonexistent", &settings, None);
        assert!(filter.is_err());
    }
}
