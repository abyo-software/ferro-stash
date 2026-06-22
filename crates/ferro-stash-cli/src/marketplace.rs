// SPDX-License-Identifier: Apache-2.0
//! AWS Marketplace metered-container entitlement gate.
//!
//! This module is compiled in ONLY by the `marketplace` cargo feature, which is
//! OFF by default. The OSS source build and the AMI product build never include
//! it, so they have no AWS Marketplace dependency and no runtime behaviour
//! change. It exists for the PAID, RegisterUsage-metered CONTAINER image that we
//! publish to AWS Marketplace (ContainerProduct@1.0, ExternallyMetered "Hours"
//! dimension): AWS meters per running pod-hour and the image proves it is a
//! legitimately subscribed copy by calling `RegisterUsage` exactly once at
//! startup.
//!
//! Runtime activation is driven by the environment, NOT baked into the binary,
//! because the product code does not exist until the listing is created:
//!
//!   * `FERROSTASH_MARKETPLACE_PRODUCT_CODE` — the Marketplace product code.
//!     UNSET (or blank) => the check is SKIPPED entirely, so a
//!     `marketplace`-feature binary still runs in local dev / CI.
//!   * AWS region — resolved from the standard AWS region provider chain
//!     (`AWS_REGION` / `AWS_DEFAULT_REGION` / profile / instance metadata) via
//!     `aws-config`; nothing region-specific is hard-coded here.
//!   * The RegisterUsage public key version is fixed at 1 (the only value AWS
//!     defines for this entitlement model today).
//!
//! Fail-closed contract (see [`Outcome`] / [`decide`]):
//!
//! * SUCCESS -> entitled; continue and start the pipeline.
//! * CustomerNotEntitled / InvalidProductCode / etc. -> a DEFINITIVE "not
//!   allowed to run" answer; log and exit NON-ZERO (fail closed). A
//!   CustomerNotEntitled is NEVER treated as success.
//! * transient (network / throttle / internal / timeout / unknown) ->
//!   inconclusive; retry a bounded number of times and then exit NON-ZERO
//!   (fail closed). Marketplace containers must prove entitlement before
//!   starting the pipeline.

use std::time::Duration;

// NOTE: this gate runs as the very first statement in `main`, BEFORE the
// `tracing` subscriber is installed, so it writes directly to stderr with
// `eprintln!` (tracing events emitted now would be dropped — no subscriber yet).
// The fail-closed reason must always be visible for operators debugging a pod.

/// Environment variable that carries the Marketplace product code at runtime.
/// Injected into the pod by the Helm chart (`marketplace.productCode`).
const PRODUCT_CODE_ENV: &str = "FERROSTASH_MARKETPLACE_PRODUCT_CODE";

/// RegisterUsage public key version. AWS defines `1` for this model.
const PUBLIC_KEY_VERSION: i32 = 1;

/// Bounded retry budget for inconclusive (transient) RegisterUsage failures.
const MAX_ATTEMPTS: u32 = 3;

/// Process exit code used when the entitlement check fails closed.
const NOT_ENTITLED_EXIT_CODE: i32 = 2;

/// The category of an entitlement-check result, kept deliberately free of any
/// AWS SDK type so the decision logic is pure and unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// No usable product code configured — running in dev / unmetered mode.
    Unset,
    /// RegisterUsage returned success — this copy is entitled.
    Entitled,
    /// AWS gave a definitive "not entitled / misconfigured product" answer.
    NotEntitled,
    /// Could not obtain a definitive answer (network/throttle/internal/timeout).
    Transient,
}

/// What the process should do, derived purely from an [`Outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Start the pipeline.
    Continue,
    /// Log and exit non-zero (fail closed).
    FailClosed,
}

/// Pure mapping from a result category to a process decision.
///
/// Only an unset product code skips the Marketplace check. Once a product code
/// is configured, both explicit denials and inconclusive RegisterUsage failures
/// fail closed.
pub(crate) fn decide(outcome: Outcome) -> Decision {
    match outcome {
        Outcome::Unset | Outcome::Entitled => Decision::Continue,
        Outcome::NotEntitled | Outcome::Transient => Decision::FailClosed,
    }
}

/// Decide, from the product-code env value alone, whether the live AWS call is
/// needed. Returns `Some(Outcome::Unset)` when there is nothing to check (so the
/// caller skips AWS entirely); returns `None` when a product code is present and
/// RegisterUsage must actually be called.
fn outcome_for_product_code(product_code: Option<&str>) -> Option<Outcome> {
    match product_code {
        Some(pc) if !pc.trim().is_empty() => None,
        _ => Some(Outcome::Unset),
    }
}

/// Entry point: run the entitlement gate, exiting the process non-zero if this
/// copy is definitively not entitled. Called once at the very start of `main`.
pub async fn check_entitlement_or_exit() {
    let product_code = std::env::var(PRODUCT_CODE_ENV).ok();
    if outcome_for_product_code(product_code.as_deref()).is_some() {
        eprintln!(
            "ferro-stash: marketplace entitlement check skipped ({PRODUCT_CODE_ENV} unset; \
             dev / unmetered mode)"
        );
        return;
    }
    // Safe: outcome_for_product_code returned None, so the value is Some+non-blank.
    let product_code = product_code.unwrap_or_default();
    let product_code = product_code.trim();

    let outcome = register_usage(product_code).await;
    match decide(outcome) {
        Decision::Continue => match outcome {
            Outcome::Entitled => {
                eprintln!(
                    "ferro-stash: marketplace entitlement verified (RegisterUsage succeeded)"
                );
            }
            // Unset is short-circuited above; Entitled is the only Continue
            // outcome that reaches here.
            Outcome::Unset | Outcome::NotEntitled | Outcome::Transient => {}
        },
        Decision::FailClosed => {
            eprintln!(
                "ferro-stash: marketplace entitlement check FAILED: entitlement could not be \
                 verified. Exiting. Subscribe to the product in AWS Marketplace and ensure the pod \
                 has the correct product code, AWS credentials/region, and network access to AWS \
                 Marketplace Metering."
            );
            std::process::exit(NOT_ENTITLED_EXIT_CODE);
        }
    }
}

/// Call AWS Marketplace Metering `RegisterUsage` (with a bounded retry on
/// transient failures) and reduce the result to an [`Outcome`].
async fn register_usage(product_code: &str) -> Outcome {
    use aws_sdk_marketplacemetering::error::SdkError;

    // Region + credentials come from the standard AWS provider chain.
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_marketplacemetering::Client::new(&config);

    let mut last = Outcome::Transient;
    for attempt in 1..=MAX_ATTEMPTS {
        match client
            .register_usage()
            .product_code(product_code)
            .public_key_version(PUBLIC_KEY_VERSION)
            .send()
            .await
        {
            Ok(_) => return Outcome::Entitled,
            Err(err) => {
                // An SdkError is either a modeled service error (which carries an
                // entitlement verdict) or a transport-level failure (timeout,
                // dispatch, response parse) which is always inconclusive.
                let outcome = match &err {
                    SdkError::ServiceError(ctx) => classify_service_error(ctx.err()),
                    _ => Outcome::Transient,
                };
                // A definitive denial is final — never retry it into a "maybe".
                if outcome == Outcome::NotEntitled {
                    eprintln!(
                        "ferro-stash: RegisterUsage returned a definitive not-entitled error: {err}"
                    );
                    return Outcome::NotEntitled;
                }
                last = outcome;
                if attempt < MAX_ATTEMPTS {
                    let backoff = Duration::from_millis(500 * u64::from(attempt));
                    eprintln!(
                        "ferro-stash: RegisterUsage transient error (attempt {attempt}/{MAX_ATTEMPTS}); \
                         retrying in {backoff:?}: {err}"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
    last
}

/// Map a modeled `RegisterUsage` service error to an [`Outcome`].
///
/// Definitive "you may not run this" errors fail closed; service-side / quota
/// errors are transient and fail closed after bounded retry. The enum is
/// `#[non_exhaustive]`; any future variant we don't recognise is treated as
/// transient rather than as a silent success.
fn classify_service_error(
    err: &aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError,
) -> Outcome {
    use aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError as E;
    match err {
        E::CustomerNotEntitledException(_)
        | E::InvalidProductCodeException(_)
        | E::InvalidPublicKeyVersionException(_)
        | E::InvalidRegionException(_)
        | E::PlatformNotSupportedException(_)
        | E::DisabledApiException(_) => Outcome::NotEntitled,
        E::InternalServiceErrorException(_) | E::ThrottlingException(_) => Outcome::Transient,
        _ => Outcome::Transient,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_continues_when_entitled() {
        assert_eq!(decide(Outcome::Entitled), Decision::Continue);
    }

    #[test]
    fn decide_fails_closed_when_not_entitled() {
        assert_eq!(decide(Outcome::NotEntitled), Decision::FailClosed);
    }

    #[test]
    fn decide_continues_when_unset() {
        assert_eq!(decide(Outcome::Unset), Decision::Continue);
    }

    #[test]
    fn decide_fails_closed_on_transient() {
        assert_eq!(decide(Outcome::Transient), Decision::FailClosed);
    }

    #[test]
    fn unset_product_code_skips_the_call() {
        assert_eq!(outcome_for_product_code(None), Some(Outcome::Unset));
        assert_eq!(outcome_for_product_code(Some("")), Some(Outcome::Unset));
        assert_eq!(outcome_for_product_code(Some("   ")), Some(Outcome::Unset));
    }

    #[test]
    fn present_product_code_triggers_the_call() {
        // None => "go make the live RegisterUsage call".
        assert_eq!(outcome_for_product_code(Some("abc123productcode")), None);
    }
}
