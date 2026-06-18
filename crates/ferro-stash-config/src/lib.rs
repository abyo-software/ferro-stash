// SPDX-License-Identifier: Apache-2.0
//! Configuration parser for `FerroStash`.
//!
//! Supports two configuration formats:
//! 1. Logstash DSL — compatible with existing Logstash config files
//! 2. YAML — modern alternative with the same semantics
//!
//! Logstash-compatible environment variable expansion (`${VAR}`, `${VAR:default}`)
//! is applied to the raw config string before parsing.

pub mod logstash_dsl;
pub mod model;
pub mod yaml_config;

pub use model::{
    Config, DlqConfigSettings, FilterConfig, InputConfig, OutputConfig, PipelineSettings,
    QueueConfig,
};

use ferro_stash_core::error::{FerroStashError, Result};

/// Expands `${VAR}` and `${VAR:default}` environment variable references in a string.
///
/// This matches Logstash behavior: config values can reference environment variables
/// that are substituted before the config is parsed.
pub fn expand_env_vars(input: &str) -> String {
    let re = regex::Regex::new(r"\$\{([^}:]+)(?::([^}]*))?\}").expect("valid regex");
    re.replace_all(input, |caps: &regex::Captures| {
        let var_name = &caps[1];
        let default = caps.get(2).map(|m| m.as_str());
        std::env::var(var_name).unwrap_or_else(|_| default.unwrap_or("").to_string())
    })
    .to_string()
}

/// Parses a configuration string with environment variable expansion.
pub fn parse_config(content: &str, format: ConfigFormat) -> Result<Config> {
    let expanded = expand_env_vars(content);
    match format {
        ConfigFormat::LogstashDsl => logstash_dsl::parse(&expanded),
        ConfigFormat::Yaml => yaml_config::parse(&expanded),
        ConfigFormat::Auto => {
            // Try YAML first, then Logstash DSL
            yaml_config::parse(&expanded).or_else(|_| logstash_dsl::parse(&expanded))
        }
    }
}

/// Parses a configuration file, detecting format from extension.
pub fn parse_config_file(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| FerroStashError::Config(format!("cannot read config file {path}: {e}")))?;

    let format = if path.ends_with(".yml") || path.ends_with(".yaml") {
        ConfigFormat::Yaml
    } else if path.ends_with(".conf") {
        ConfigFormat::LogstashDsl
    } else {
        ConfigFormat::Auto
    };

    parse_config(&content, format)
}

/// Configuration format.
#[derive(Debug, Clone, Copy)]
pub enum ConfigFormat {
    LogstashDsl,
    Yaml,
    Auto,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_env_vars_with_value() {
        std::env::set_var("FERROSTASH_TEST_PORT", "5555");
        let result = expand_env_vars("port => ${FERROSTASH_TEST_PORT}");
        assert_eq!(result, "port => 5555");
        std::env::remove_var("FERROSTASH_TEST_PORT");
    }

    #[test]
    fn test_expand_env_vars_with_default() {
        std::env::remove_var("FERROSTASH_MISSING_VAR");
        let result = expand_env_vars("port => ${FERROSTASH_MISSING_VAR:9200}");
        assert_eq!(result, "port => 9200");
    }

    #[test]
    fn test_expand_env_vars_missing_no_default() {
        std::env::remove_var("FERROSTASH_MISSING_VAR2");
        let result = expand_env_vars("host => ${FERROSTASH_MISSING_VAR2}");
        assert_eq!(result, "host => ");
    }

    #[test]
    fn test_expand_env_vars_no_vars() {
        let result = expand_env_vars("host => localhost");
        assert_eq!(result, "host => localhost");
    }

    #[test]
    fn test_expand_env_vars_multiple() {
        std::env::set_var("FERROSTASH_HOST", "myhost");
        std::env::set_var("FERROSTASH_PORT", "9200");
        let result = expand_env_vars("${FERROSTASH_HOST}:${FERROSTASH_PORT}");
        assert_eq!(result, "myhost:9200");
        std::env::remove_var("FERROSTASH_HOST");
        std::env::remove_var("FERROSTASH_PORT");
    }
}
