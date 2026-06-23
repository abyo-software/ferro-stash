// SPDX-License-Identifier: Apache-2.0
//! AWS Marketplace metered-container entitlement gate + hourly usage metering.
//!
//! This module is compiled in ONLY by the `marketplace` cargo feature, which is
//! OFF by default. The OSS source build and the AMI product build never include
//! it, so they have no AWS Marketplace dependency and no runtime behaviour
//! change. It exists for the PAID, metered CONTAINER image that we publish to
//! AWS Marketplace (ContainerProduct@1.0, a single **ExternallyMetered** "Hours"
//! dimension): AWS bills per running pod-hour and the image must both (1) prove
//! it is a legitimately subscribed copy and (2) actually emit usage records.
//!
//! There are therefore TWO marketplace calls, both against
//! [`aws-sdk-marketplacemetering`]:
//!
//!   1. **`RegisterUsage` at startup** (see [`check_entitlement_or_exit`] /
//!      [`decide`]): the container-appropriate entitlement gate, called exactly
//!      once before the pipeline starts. Fail-closed.
//!   2. **`MeterUsage` hourly** (see [`run_metering_loop`]): an ExternallyMetered
//!      dimension is metered *by the application*, not by AWS. Without a
//!      `MeterUsage` record the "Hours" dimension produces no billing — which is
//!      exactly why AWS rejected the container's Public submission. After the
//!      entitlement gate passes a detached background task calls `MeterUsage`
//!      once immediately (so the integration is observable within seconds of
//!      boot — AWS verifies a record arrives before approving the listing) and
//!      then once per hour. It does NOT block pipeline startup.
//!
//! Runtime activation is driven by the environment, NOT baked into the binary,
//! because the product code does not exist until the listing is created:
//!
//!   * `FERROSTASH_MARKETPLACE_PRODUCT_CODE` — the Marketplace product code.
//!     UNSET (or blank) => both the entitlement check AND the metering loop are
//!     SKIPPED entirely, so a `marketplace`-feature binary still runs in local
//!     dev / CI.
//!   * AWS region — resolved from the standard AWS region provider chain
//!     (`AWS_REGION` / `AWS_DEFAULT_REGION` / profile / instance metadata) via
//!     `aws-config`; nothing region-specific is hard-coded here.
//!   * The RegisterUsage public key version is fixed at 1 (the only value AWS
//!     defines for this entitlement model today).
//!
//! ### Hourly `MeterUsage` design (see [`run_metering_loop`])
//!
//! * **Idempotent per hour bucket.** `MeterUsage` is idempotent per (product,
//!   dimension, hour): each call is stamped at the **start-of-hour** epoch
//!   ([`hour_bucket_secs`]), so a retry or a same-hour restart re-sends in the
//!   SAME bucket and AWS returns `DuplicateRequestException`, which is treated as
//!   an idempotent success — never a double-bill. Calls are aligned to the hour
//!   boundary ([`secs_until_next_hour`]) so each hour produces exactly one
//!   record.
//! * **Dimension "Hours", quantity 1.** One pod-hour per elapsed hour.
//! * **Entitlement enforced through `MeterUsage` too.** A terminal `MeterUsage`
//!   error (CustomerNotEntitled, InvalidProductCode, invalid dimension/timestamp,
//!   …) is fatal: the loop exits the process NON-ZERO (the same fail-closed
//!   posture the startup gate takes). A transient error (throttle / internal /
//!   network / timeout) retries with bounded backoff and never kills the process.
//!
//! Fail-closed contract (see [`Outcome`] / [`decide`] for startup, [`MeterAction`]
//! for the hourly loop):
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

/// The single ExternallyMetered billing dimension on the container listing.
/// Must match the Marketplace rate-card dimension API name EXACTLY ("Hours");
/// a mismatch makes `MeterUsage` fail terminally (invalid dimension).
const USAGE_DIMENSION: &str = "Hours";

/// Quantity metered per elapsed hour: one pod-hour per hour bucket.
const HOURLY_QUANTITY: i32 = 1;

/// Seconds in an hour (the `MeterUsage` aggregation/idempotency window).
const SECS_PER_HOUR: i64 = 3600;

/// Bounded retry budget for inconclusive (transient) RegisterUsage / MeterUsage
/// failures.
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

/// Specific reason behind a `NotEntitled` outcome, kept SDK-free so we can
/// surface an actionable hint to the operator without leaking the verbose
/// AWS error display into a buyer-facing message. `Other` is a catch-all for
/// new modeled errors AWS may add later (the enum is `#[non_exhaustive]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DenialReason {
    CustomerNotEntitled,
    InvalidProductCode,
    InvalidPublicKeyVersion,
    InvalidRegion,
    PlatformNotSupported,
    DisabledApi,
    Other,
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

/// Outcome of one full RegisterUsage attempt sequence — the [`Outcome`]
/// category plus diagnostic context (the specific denial reason for a
/// definitive denial, or the last error string for a transient failure) so
/// the fail-closed message can tell the operator *what* to fix rather than
/// printing the same generic "could not be verified" line for every failure
/// mode (1.0.1 buyer-UX gap).
struct RegisterResult {
    outcome: Outcome,
    /// Set only when `outcome == NotEntitled`.
    denial: Option<DenialReason>,
    /// The most recent SDK error string (any outcome that wasn't immediate
    /// success). Truncated for log hygiene.
    last_error: Option<String>,
}

/// Operator-facing hint for a specific denial reason. Stays short, actionable,
/// and free of AWS SDK jargon — these strings appear in the buyer's pod logs.
fn denial_hint(reason: DenialReason) -> &'static str {
    match reason {
        DenialReason::CustomerNotEntitled => {
            "this AWS account is not subscribed to the product. \
             Subscribe in AWS Marketplace and retry."
        }
        DenialReason::InvalidProductCode => {
            "the configured FERROSTASH_MARKETPLACE_PRODUCT_CODE is not a \
             valid Marketplace product code for this listing. \
             Check the Helm chart's marketplace.productCode value."
        }
        DenialReason::InvalidPublicKeyVersion => {
            "the RegisterUsage public-key version is not accepted (AWS \
             today defines only version 1). Upgrade ferro-stash."
        }
        DenialReason::InvalidRegion => {
            "this AWS region is not supported for the listing. \
             Set AWS_REGION to a region the product is published in \
             (see the listing's region availability)."
        }
        DenialReason::PlatformNotSupported => {
            "the runtime platform is not eligible for AWS Marketplace \
             metering (the container must run on Amazon EKS with IRSA \
             or Pod Identity granting aws-marketplace:RegisterUsage)."
        }
        DenialReason::DisabledApi => "AWS Marketplace Metering API is disabled for this account.",
        DenialReason::Other => {
            "AWS Marketplace returned a definitive denial. See the error \
             line above for the AWS error code."
        }
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

    let result = register_usage(product_code).await;
    match decide(result.outcome) {
        Decision::Continue => match result.outcome {
            Outcome::Entitled => {
                eprintln!(
                    "ferro-stash: marketplace entitlement verified (RegisterUsage succeeded)"
                );
                // The "Hours" dimension is ExternallyMetered: AWS only bills it
                // once the application emits `MeterUsage` records. The product
                // code is present + non-blank here (we only reach RegisterUsage
                // with a configured code), so start the hourly billing loop. It
                // is detached and does NOT block pipeline startup.
                spawn_metering(product_code.to_string());
            }
            // Unset is short-circuited above; Entitled is the only Continue
            // outcome that reaches here.
            Outcome::Unset | Outcome::NotEntitled | Outcome::Transient => {}
        },
        Decision::FailClosed => {
            // Distinguish a *definitive* denial (operator must change subscription /
            // config) from an *inconclusive* failure (network/throttle, retry-able).
            // The 1.0.1 build collapsed both into a single generic line, leaving
            // the operator guessing whether to subscribe, fix env vars, or check
            // egress.
            match result.outcome {
                Outcome::NotEntitled => {
                    let reason = result.denial.unwrap_or(DenialReason::Other);
                    eprintln!(
                        "ferro-stash: marketplace entitlement check FAILED (denied: {reason:?}): {hint} \
                         AWS error: {err}",
                        hint = denial_hint(reason),
                        err = result.last_error.as_deref().unwrap_or("<no detail>"),
                    );
                }
                Outcome::Transient => {
                    eprintln!(
                        "ferro-stash: marketplace entitlement check FAILED (could not reach AWS \
                         Marketplace Metering after {MAX_ATTEMPTS} attempts). Verify the pod has \
                         egress to *.marketplacemetering.amazonaws.com on 443, AWS credentials \
                         (IRSA / Pod Identity) with aws-marketplace:RegisterUsage, and a valid \
                         AWS_REGION. Last error: {err}",
                        err = result.last_error.as_deref().unwrap_or("<no detail>"),
                    );
                }
                // Decision::FailClosed only fires for NotEntitled / Transient.
                Outcome::Unset | Outcome::Entitled => unreachable!(),
            }
            std::process::exit(NOT_ENTITLED_EXIT_CODE);
        }
    }
}

/// Call AWS Marketplace Metering `RegisterUsage` (with a bounded retry on
/// transient failures) and reduce the result to a [`RegisterResult`] —
/// outcome + the most-specific denial reason + a short rendering of the
/// triggering SDK error, so the caller can build an actionable fail-closed
/// message instead of the generic "could not be verified" line that 1.0.1
/// shipped.
async fn register_usage(product_code: &str) -> RegisterResult {
    use aws_sdk_marketplacemetering::error::SdkError;

    // Region + credentials come from the standard AWS provider chain.
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_marketplacemetering::Client::new(&config);

    let mut last_outcome = Outcome::Transient;
    let mut last_error: Option<String> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match client
            .register_usage()
            .product_code(product_code)
            .public_key_version(PUBLIC_KEY_VERSION)
            .send()
            .await
        {
            Ok(_) => {
                return RegisterResult {
                    outcome: Outcome::Entitled,
                    denial: None,
                    last_error: None,
                };
            }
            Err(err) => {
                // An SdkError is either a modeled service error (which carries an
                // entitlement verdict) or a transport-level failure (timeout,
                // dispatch, response parse) which is always inconclusive.
                let (outcome, denial) = match &err {
                    SdkError::ServiceError(ctx) => {
                        let denial = denial_reason_for(ctx.err());
                        let outcome = classify_service_error(ctx.err());
                        (outcome, denial)
                    }
                    _ => (Outcome::Transient, None),
                };
                last_error = Some(short_err(&err));
                // A definitive denial is final — never retry it into a "maybe".
                if outcome == Outcome::NotEntitled {
                    let reason = denial.unwrap_or(DenialReason::Other);
                    eprintln!(
                        "ferro-stash: RegisterUsage returned a definitive denial ({reason:?}): {err}"
                    );
                    return RegisterResult {
                        outcome: Outcome::NotEntitled,
                        denial: Some(reason),
                        last_error,
                    };
                }
                last_outcome = outcome;
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
    RegisterResult {
        outcome: last_outcome,
        denial: None,
        last_error,
    }
}

/// Render an SDK error to a single short line for log/UX use.
///
/// `SdkError`'s top-level `Display` is just "service error" / "dispatch
/// failure" with no detail — useless on its own. The actual error message
/// (e.g. "You are not subscribed", a region name, a credential failure
/// reason) lives in the cause chain. We walk `source()` to surface the
/// deepest "; cause: …" link, trimmed for log hygiene so a fail-closed line
/// stays readable in `kubectl logs` / CloudWatch UI.
fn short_err<E>(err: &E) -> String
where
    E: std::error::Error,
{
    const MAX: usize = 320;
    let mut parts = vec![err.to_string()];
    let mut src: Option<&dyn std::error::Error> = err.source();
    let mut seen = 0;
    while let Some(e) = src {
        parts.push(e.to_string());
        src = e.source();
        seen += 1;
        if seen >= 4 {
            // Defensive cap: deep cause chains do exist (transport → tls →
            // io) but four levels is plenty of context without flooding logs.
            break;
        }
    }
    let s = parts.join(": ");
    if s.len() <= MAX {
        s
    } else {
        format!("{}... [truncated]", &s[..MAX])
    }
}

/// Map a modeled RegisterUsage service error to its [`DenialReason`] only when
/// the error is in the "definitive denial" set; returns `None` for transient
/// service errors (those have no operator-facing denial reason).
fn denial_reason_for(
    err: &aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError,
) -> Option<DenialReason> {
    use aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError as E;
    match err {
        E::CustomerNotEntitledException(_) => Some(DenialReason::CustomerNotEntitled),
        E::InvalidProductCodeException(_) => Some(DenialReason::InvalidProductCode),
        E::InvalidPublicKeyVersionException(_) => Some(DenialReason::InvalidPublicKeyVersion),
        E::InvalidRegionException(_) => Some(DenialReason::InvalidRegion),
        E::PlatformNotSupportedException(_) => Some(DenialReason::PlatformNotSupported),
        E::DisabledApiException(_) => Some(DenialReason::DisabledApi),
        // Transient (Internal/Throttling) or future variants: no denial reason.
        _ => None,
    }
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

// ---------------------------------------------------------------------------
// Hourly MeterUsage (ExternallyMetered "Hours" dimension)
// ---------------------------------------------------------------------------

/// The category of a single `MeterUsage` attempt, kept deliberately free of any
/// AWS SDK type so the decision logic is pure and unit-testable (mirrors
/// [`Outcome`] for the RegisterUsage gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MeterOutcome {
    /// A usage record was produced for this hour bucket.
    Metered,
    /// AWS reports this (product, dimension, hour) is already metered — an
    /// idempotent duplicate from a same-hour retry/restart. The hour is already
    /// accounted, so this is billing-safe and treated as success.
    Duplicate,
    /// A definitive error that keeps failing until an operator intervenes (not
    /// entitled, invalid product/dimension, timestamp out of bounds, …).
    Terminal,
    /// Inconclusive (throttle / internal / network / timeout) — retry/backoff.
    Transient,
}

/// What the hourly metering loop should do after one attempt, derived purely
/// from a [`MeterOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MeterAction {
    /// The hour is accounted (metered or an idempotent duplicate) — stop
    /// retrying and sleep until the next hour boundary.
    Advance,
    /// Retry the SAME hour bucket after a backoff (transient).
    Retry,
    /// Log and exit the process non-zero (fail closed) — consistent with the
    /// startup gate's entitlement posture.
    FailClosed,
}

/// Pure mapping from a meter outcome to the loop's next action.
///
/// A success or an idempotent duplicate advances; a transient error retries; a
/// terminal error fails closed. Transient NEVER fails closed here (unlike the
/// startup gate's [`decide`], where a transient that exhausts retries also fails
/// closed) — the running container must not be killed by a passing AWS blip.
pub(crate) fn meter_action(outcome: MeterOutcome) -> MeterAction {
    match outcome {
        MeterOutcome::Metered | MeterOutcome::Duplicate => MeterAction::Advance,
        MeterOutcome::Transient => MeterAction::Retry,
        MeterOutcome::Terminal => MeterAction::FailClosed,
    }
}

/// Round an epoch-seconds instant down to the start of its hour (UTC).
///
/// `MeterUsage` meters a per-hour aggregate and is **idempotent per (product,
/// dimension, hour)**. Stamping every call at the hour boundary (not raw
/// wall-clock) means a retry or a post-restart re-send within the same hour
/// lands in the SAME bucket, so AWS recognises it as a duplicate (idempotent)
/// instead of billing it twice. (Raw wall-clock timestamps would make two
/// flushes minutes apart look like two *distinct* records and could double-bill
/// across a restart.)
fn hour_bucket_secs(now_secs: i64) -> i64 {
    now_secs.div_euclid(SECS_PER_HOUR) * SECS_PER_HOUR
}

/// Seconds from `now_secs` until the next hour boundary, always in `1..=3600`.
/// Used to align each hourly `MeterUsage` to the top of the hour so every hour
/// produces exactly one record. On the boundary itself it returns a full hour.
fn secs_until_next_hour(now_secs: i64) -> i64 {
    SECS_PER_HOUR - now_secs.rem_euclid(SECS_PER_HOUR)
}

/// Current wall-clock as epoch seconds (0 if the clock predates the epoch).
fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Map a modeled `MeterUsage` service error to a [`MeterOutcome`].
///
/// Definitive "this will keep failing" errors (lost entitlement, bad product /
/// dimension, timestamp out of bounds, …) are terminal and fail closed — the
/// same posture the startup `RegisterUsage` gate takes. Throttling / internal
/// errors are transient and retried. A `DuplicateRequestException` is an
/// idempotent success (the hour is already metered). The enum is
/// `#[non_exhaustive]`; an unrecognised future variant is treated as transient
/// (retry) rather than killing a healthy process — mirroring
/// [`classify_service_error`]'s "unknown is not a silent verdict" default.
fn classify_meter_error(
    err: &aws_sdk_marketplacemetering::operation::meter_usage::MeterUsageError,
) -> MeterOutcome {
    use aws_sdk_marketplacemetering::operation::meter_usage::MeterUsageError as E;
    match err {
        E::DuplicateRequestException(_) => MeterOutcome::Duplicate,
        E::CustomerNotEntitledException(_)
        | E::InvalidProductCodeException(_)
        | E::InvalidEndpointRegionException(_)
        | E::InvalidUsageDimensionException(_)
        | E::InvalidUsageAllocationsException(_)
        | E::InvalidTagException(_)
        | E::IdempotencyConflictException(_)
        | E::TimestampOutOfBoundsException(_) => MeterOutcome::Terminal,
        E::InternalServiceErrorException(_) | E::ThrottlingException(_) => MeterOutcome::Transient,
        _ => MeterOutcome::Transient,
    }
}

/// Issue exactly ONE `MeterUsage` call for `timestamp`'s hour bucket (dimension
/// [`USAGE_DIMENSION`], quantity [`HOURLY_QUANTITY`]) and reduce the result to a
/// [`MeterOutcome`]. A transport-level failure (timeout / dispatch / parse) is
/// always inconclusive → [`MeterOutcome::Transient`].
async fn meter_usage_once(
    client: &aws_sdk_marketplacemetering::Client,
    product_code: &str,
    timestamp: aws_sdk_marketplacemetering::primitives::DateTime,
) -> MeterOutcome {
    use aws_sdk_marketplacemetering::error::SdkError;
    match client
        .meter_usage()
        .product_code(product_code)
        .timestamp(timestamp)
        .usage_dimension(USAGE_DIMENSION)
        .usage_quantity(HOURLY_QUANTITY)
        .send()
        .await
    {
        Ok(_) => MeterOutcome::Metered,
        Err(err) => match &err {
            SdkError::ServiceError(ctx) => classify_meter_error(ctx.err()),
            // Any non-service (transport) error is inconclusive → retry.
            _ => MeterOutcome::Transient,
        },
    }
}

/// Background billing loop for the ExternallyMetered "Hours" dimension.
///
/// Emits `MeterUsage` once IMMEDIATELY (so the integration is observable within
/// seconds of boot — AWS verifies a record arrives before approving the listing)
/// and then once per hour, aligned to the hour boundary and stamped at the hour
/// bucket so retries/restarts within an hour are idempotent duplicates. A
/// terminal error fails closed (process exit, consistent with the startup gate);
/// a transient error retries with bounded backoff and never kills the process.
async fn run_metering_loop(product_code: String) {
    // Region + credentials from the standard AWS provider chain (same as the
    // RegisterUsage gate); the client is built once and reused every hour.
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_marketplacemetering::Client::new(&config);

    loop {
        let bucket = hour_bucket_secs(now_epoch_secs());
        let timestamp = aws_sdk_marketplacemetering::primitives::DateTime::from_secs(bucket);

        // Bounded transient retries within this hour bucket. A terminal outcome
        // fails closed; a persistent transient is logged and this hour's record
        // is skipped (the safe under-bill direction) rather than killing the
        // process — next hour gets a fresh bucket and a fresh attempt.
        for attempt in 1..=MAX_ATTEMPTS {
            let outcome = meter_usage_once(&client, &product_code, timestamp).await;
            match meter_action(outcome) {
                MeterAction::Advance => {
                    if outcome == MeterOutcome::Duplicate {
                        tracing::info!(
                            product_code = %product_code,
                            "MeterUsage duplicate (already metered this hour); idempotent success",
                        );
                    } else {
                        tracing::debug!(
                            product_code = %product_code,
                            dimension = USAGE_DIMENSION,
                            "MeterUsage recorded one pod-hour",
                        );
                    }
                    break;
                }
                MeterAction::Retry => {
                    if attempt < MAX_ATTEMPTS {
                        let backoff = Duration::from_millis(500 * u64::from(attempt));
                        tracing::warn!(
                            product_code = %product_code,
                            attempt,
                            max = MAX_ATTEMPTS,
                            "MeterUsage transient error; retrying in {backoff:?}",
                        );
                        tokio::time::sleep(backoff).await;
                    } else {
                        tracing::warn!(
                            product_code = %product_code,
                            "MeterUsage still transient after {MAX_ATTEMPTS} attempts; skipping \
                             this hour's record (process stays up; usage retried next hour)",
                        );
                    }
                }
                MeterAction::FailClosed => {
                    // NOTE: this can fire before the tracing subscriber is up (a
                    // terminal error on the immediate boot call), so write the
                    // operator-facing reason directly to stderr like the gate.
                    eprintln!(
                        "ferro-stash: MeterUsage returned a definitive error for dimension \
                         '{USAGE_DIMENSION}'; this copy can no longer meter usage. Exiting (fail \
                         closed). Verify the AWS Marketplace subscription, product code, and \
                         region/credentials."
                    );
                    std::process::exit(NOT_ENTITLED_EXIT_CODE);
                }
            }
        }

        // Align to the next hour boundary so each hour produces exactly one
        // record. `secs_until_next_hour` is always >= 1, so this never busy-loops.
        let sleep_secs = secs_until_next_hour(now_epoch_secs());
        tokio::time::sleep(Duration::from_secs(sleep_secs.unsigned_abs())).await;
    }
}

/// Spawn the detached hourly `MeterUsage` billing loop. It lives for the process
/// lifetime and must NOT block pipeline startup (hence a detached `tokio::spawn`,
/// not an awaited future).
fn spawn_metering(product_code: String) {
    tokio::spawn(run_metering_loop(product_code));
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

    // -----------------------------------------------------------------------
    // Denial-reason classification + hints (fail-closed message specificity)
    // -----------------------------------------------------------------------

    #[test]
    fn denial_reason_for_each_modeled_denial_variant() {
        use aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError;
        use aws_sdk_marketplacemetering::types::error::{
            CustomerNotEntitledException, DisabledApiException, InvalidProductCodeException,
            InvalidPublicKeyVersionException, InvalidRegionException,
            PlatformNotSupportedException,
        };

        let cases: Vec<(RegisterUsageError, DenialReason)> = vec![
            (
                RegisterUsageError::CustomerNotEntitledException(
                    CustomerNotEntitledException::builder().build(),
                ),
                DenialReason::CustomerNotEntitled,
            ),
            (
                RegisterUsageError::InvalidProductCodeException(
                    InvalidProductCodeException::builder().build(),
                ),
                DenialReason::InvalidProductCode,
            ),
            (
                RegisterUsageError::InvalidPublicKeyVersionException(
                    InvalidPublicKeyVersionException::builder().build(),
                ),
                DenialReason::InvalidPublicKeyVersion,
            ),
            (
                RegisterUsageError::InvalidRegionException(
                    InvalidRegionException::builder().build(),
                ),
                DenialReason::InvalidRegion,
            ),
            (
                RegisterUsageError::PlatformNotSupportedException(
                    PlatformNotSupportedException::builder().build(),
                ),
                DenialReason::PlatformNotSupported,
            ),
            (
                RegisterUsageError::DisabledApiException(DisabledApiException::builder().build()),
                DenialReason::DisabledApi,
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(
                denial_reason_for(&err),
                Some(expected),
                "denial classification mismatch"
            );
        }
    }

    #[test]
    fn denial_reason_none_for_transient_service_errors() {
        use aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError;
        use aws_sdk_marketplacemetering::types::error::{
            InternalServiceErrorException, ThrottlingException,
        };
        assert!(denial_reason_for(&RegisterUsageError::ThrottlingException(
            ThrottlingException::builder().build()
        ))
        .is_none());
        assert!(
            denial_reason_for(&RegisterUsageError::InternalServiceErrorException(
                InternalServiceErrorException::builder().build()
            ))
            .is_none()
        );
    }

    #[test]
    fn denial_hint_text_is_actionable_for_every_reason() {
        // Each denial reason gets a non-empty operator-facing hint. Future
        // additions to DenialReason MUST add a hint here too (the compiler
        // will surface a missing arm via the `match` in `denial_hint`).
        for reason in [
            DenialReason::CustomerNotEntitled,
            DenialReason::InvalidProductCode,
            DenialReason::InvalidPublicKeyVersion,
            DenialReason::InvalidRegion,
            DenialReason::PlatformNotSupported,
            DenialReason::DisabledApi,
            DenialReason::Other,
        ] {
            let hint = denial_hint(reason);
            assert!(!hint.is_empty(), "empty hint for {reason:?}");
            // No mention of generic "could not be verified" — that was the
            // 1.0.1 buyer-UX gap we're fixing.
            assert!(
                !hint.contains("could not be verified"),
                "{reason:?} hint must be specific, not the old generic message"
            );
        }
    }

    #[test]
    fn short_err_surfaces_cause_chain_not_just_top_display() {
        // SdkError::Display alone is unhelpful ("service error" / "dispatch
        // failure"); short_err must reach the deepest source for the real
        // reason. Use a hand-built chained error to keep the test SDK-free.
        #[derive(Debug)]
        struct Wrapper {
            msg: &'static str,
            cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
        }
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.msg)
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.cause
                    .as_deref()
                    .map(|e| e as &(dyn std::error::Error + 'static))
            }
        }

        let leaf = Wrapper {
            msg: "AccessDeniedException: not authorized to call aws-marketplace:RegisterUsage",
            cause: None,
        };
        let mid = Wrapper {
            msg: "unhandled error",
            cause: Some(Box::new(leaf)),
        };
        let top = Wrapper {
            msg: "service error",
            cause: Some(Box::new(mid)),
        };
        let rendered = short_err(&top);
        assert!(
            rendered.contains("AccessDeniedException")
                && rendered.contains("aws-marketplace:RegisterUsage"),
            "short_err must surface the leaf cause; got: {rendered}"
        );
    }

    // -----------------------------------------------------------------------
    // Hourly MeterUsage: hour-bucket idempotency + terminal/transient decision
    // -----------------------------------------------------------------------

    #[test]
    fn hour_bucket_rounds_down_to_top_of_hour() {
        // 02:30:30 (since epoch) rounds down to 02:00:00.
        let two_thirty = 2 * SECS_PER_HOUR + 30 * 60 + 30;
        assert_eq!(hour_bucket_secs(two_thirty), 2 * SECS_PER_HOUR);
        // Exactly on a boundary is its own bucket; the epoch is bucket 0.
        assert_eq!(hour_bucket_secs(2 * SECS_PER_HOUR), 2 * SECS_PER_HOUR);
        assert_eq!(hour_bucket_secs(0), 0);
    }

    #[test]
    fn same_hour_sends_share_one_bucket_timestamp() {
        // Idempotency: any two instants within the same wall-clock hour stamp the
        // SAME bucket, so AWS dedups a retry/restart instead of double-billing.
        let base = 100 * SECS_PER_HOUR;
        assert_eq!(
            hour_bucket_secs(base + 5),
            hour_bucket_secs(base + (SECS_PER_HOUR - 1)),
            "two same-hour sends must carry an identical timestamp"
        );
        // The next hour is a DISTINCT bucket (a new, separately-billed record).
        assert_ne!(
            hour_bucket_secs(base + 5),
            hour_bucket_secs(base + SECS_PER_HOUR),
        );
    }

    #[test]
    fn secs_until_next_hour_stays_within_the_hour() {
        assert_eq!(secs_until_next_hour(0), SECS_PER_HOUR); // top of hour -> full hour
        assert_eq!(secs_until_next_hour(SECS_PER_HOUR), SECS_PER_HOUR);
        assert_eq!(secs_until_next_hour(SECS_PER_HOUR + 1), SECS_PER_HOUR - 1);
        assert_eq!(secs_until_next_hour(2 * SECS_PER_HOUR - 1), 1); // 1s before boundary
        for now in [1_i64, 59, 1234, 86_399, 100 * SECS_PER_HOUR + 7] {
            let d = secs_until_next_hour(now);
            assert!(
                (1..=SECS_PER_HOUR).contains(&d),
                "delay {d} out of range for now {now}",
            );
        }
    }

    #[test]
    fn meter_action_advances_on_metered_and_duplicate() {
        assert_eq!(meter_action(MeterOutcome::Metered), MeterAction::Advance);
        // A duplicate is an idempotent success: advance, NEVER fail closed.
        assert_eq!(meter_action(MeterOutcome::Duplicate), MeterAction::Advance);
    }

    #[test]
    fn meter_action_retries_on_transient_not_fail_closed() {
        // Unlike the startup gate, a transient in the running loop must NOT kill
        // the process — it retries.
        assert_eq!(meter_action(MeterOutcome::Transient), MeterAction::Retry);
    }

    #[test]
    fn meter_action_fails_closed_on_terminal() {
        assert_eq!(
            meter_action(MeterOutcome::Terminal),
            MeterAction::FailClosed
        );
    }

    #[test]
    fn classify_meter_terminal_errors_fail_closed() {
        use aws_sdk_marketplacemetering::operation::meter_usage::MeterUsageError;
        use aws_sdk_marketplacemetering::types::error::{
            CustomerNotEntitledException, InvalidProductCodeException,
            InvalidUsageDimensionException, TimestampOutOfBoundsException,
        };

        let not_entitled = MeterUsageError::CustomerNotEntitledException(
            CustomerNotEntitledException::builder()
                .message("no subscription")
                .build(),
        );
        assert_eq!(classify_meter_error(&not_entitled), MeterOutcome::Terminal);
        assert_eq!(
            meter_action(classify_meter_error(&not_entitled)),
            MeterAction::FailClosed,
            "a not-entitled MeterUsage must fail closed like the startup gate",
        );

        let bad_product = MeterUsageError::InvalidProductCodeException(
            InvalidProductCodeException::builder()
                .message("bad product code")
                .build(),
        );
        assert_eq!(classify_meter_error(&bad_product), MeterOutcome::Terminal);

        let bad_dimension = MeterUsageError::InvalidUsageDimensionException(
            InvalidUsageDimensionException::builder()
                .message("not on the rate card")
                .build(),
        );
        assert_eq!(classify_meter_error(&bad_dimension), MeterOutcome::Terminal);

        let bad_timestamp = MeterUsageError::TimestampOutOfBoundsException(
            TimestampOutOfBoundsException::builder()
                .message("too old")
                .build(),
        );
        assert_eq!(classify_meter_error(&bad_timestamp), MeterOutcome::Terminal);
    }

    #[test]
    fn classify_meter_transient_errors_retry() {
        use aws_sdk_marketplacemetering::operation::meter_usage::MeterUsageError;
        use aws_sdk_marketplacemetering::types::error::{
            InternalServiceErrorException, ThrottlingException,
        };

        let throttle = MeterUsageError::ThrottlingException(
            ThrottlingException::builder().message("slow down").build(),
        );
        assert_eq!(classify_meter_error(&throttle), MeterOutcome::Transient);
        assert_eq!(
            meter_action(classify_meter_error(&throttle)),
            MeterAction::Retry
        );

        let internal = MeterUsageError::InternalServiceErrorException(
            InternalServiceErrorException::builder()
                .message("server error")
                .build(),
        );
        assert_eq!(classify_meter_error(&internal), MeterOutcome::Transient);
    }

    #[test]
    fn classify_meter_duplicate_is_idempotent_success() {
        use aws_sdk_marketplacemetering::operation::meter_usage::MeterUsageError;
        use aws_sdk_marketplacemetering::types::error::DuplicateRequestException;

        let dup = MeterUsageError::DuplicateRequestException(
            DuplicateRequestException::builder()
                .message("already metered this hour")
                .build(),
        );
        assert_eq!(classify_meter_error(&dup), MeterOutcome::Duplicate);
        // A same-hour duplicate must advance (idempotent), never fail closed —
        // this is what makes a same-hour restart/retry safe.
        assert_eq!(
            meter_action(classify_meter_error(&dup)),
            MeterAction::Advance
        );
    }
}
