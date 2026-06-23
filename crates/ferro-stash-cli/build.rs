// SPDX-License-Identifier: Apache-2.0
//! Build script that injects a real git SHA and a build timestamp into the
//! binary, so the monitoring API's `build_sha` / `build_date` fields and the
//! `--version` line carry verifiable provenance instead of placeholders. This
//! lets an AWS Marketplace buyer who pulls the container or AMI confirm which
//! commit they actually got, and lets us correlate a deployed pod with a
//! changelog entry.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Rerun if HEAD moves or staged tree changes — otherwise cargo caches the
    // env vars across rebuilds and a fresh checkout silently reuses the prior
    // SHA. (`HEAD` covers branch/commit changes; refs/HEAD covers detached HEAD
    // after a checkout.)
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
    println!("cargo:rerun-if-env-changed=FERROSTASH_BUILD_SHA");
    println!("cargo:rerun-if-env-changed=FERROSTASH_BUILD_DATE");

    // SHA: short git hash, or a clear marker when the source tree is not a
    // git checkout (e.g. crates.io tarball, vendored builds). The honest
    // placeholder is preferred over a silent fake.
    let sha = std::env::var("FERROSTASH_BUILD_SHA")
        .ok()
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short=12", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Tree dirty marker — if any tracked file differs from HEAD, suffix the
    // SHA so a hot-patched binary cannot be mistaken for the tagged commit.
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);
    let sha = if dirty && sha != "unknown" {
        format!("{sha}-dirty")
    } else {
        sha
    };

    // Build date in RFC3339 (UTC). SystemTime is the only time source allowed
    // here; chrono would pull a build-script dep we do not otherwise need.
    let date = std::env::var("FERROSTASH_BUILD_DATE").unwrap_or_else(|_| {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        rfc3339_utc(secs)
    });

    println!("cargo:rustc-env=FERROSTASH_BUILD_SHA={sha}");
    println!("cargo:rustc-env=FERROSTASH_BUILD_DATE={date}");
}

/// Minimal RFC3339 (UTC) formatter for Unix seconds, avoiding a chrono build
/// dependency. Handles the proleptic Gregorian calendar from 1970 onward.
fn rfc3339_utc(secs: u64) -> String {
    const SECS_PER_DAY: u64 = 86_400;
    let days = secs / SECS_PER_DAY;
    let rem = secs % SECS_PER_DAY;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;

    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Convert days-since-1970-01-01 to a civil (Y, M, D) date. Algorithm from
/// Howard Hinnant's "chrono-Compatible Low-Level Date Algorithms" — handles
/// any year without leap-year edge cases.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
