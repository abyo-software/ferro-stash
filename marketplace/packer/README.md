# FerroStash -- AWS Marketplace AMI (Packer)

This directory contains the Packer build that produces the FerroStash
Marketplace AMI for **arm64 (Graviton)** and **x86_64**, on top of the
**Amazon Linux 2023** base image. The initial Marketplace product ships
**arm64 only**.

## Layout

```
marketplace/packer/
├── ferro-stash.pkr.hcl         # Multi-builder Packer build
├── variables.pkr.hcl           # Typed input variables
├── build.sh                    # End-to-end wrapper (cross + packer)
├── Cross.toml                  # `cross` image config (cmake/clang for rdkafka)
├── README.md                   # This file
├── files/
│   ├── ferro-stash.service        # Systemd unit (Type=simple, hardened)
│   ├── pipeline.conf              # Default Logstash-DSL pipeline (Beats -> file)
│   └── firstboot-systemd.service  # Once-per-AMI bootstrap unit
├── scripts/
│   ├── 00-install-deps.sh
│   ├── 10-create-ferrostash-user.sh
│   ├── 20-install-binaries.sh
│   ├── 30-install-config.sh
│   ├── 40-install-systemd-units.sh
│   ├── 50-firstboot.sh
│   ├── 60-harden.sh
│   └── 90-marketplace-finalise.sh
└── tests/
    ├── lint.sh
    └── structure.sh
```

## Build flow (high level)

1. **`build.sh`** cross-compiles the `ferro-stash` binary for
   `aarch64-unknown-linux-gnu` (default; see the rdkafka caveat below) via the
   `cross` tool, staging it under `./build/<arch>/`.
2. **Packer** spins up a build VM (default `c7g.large`) using the latest
   Amazon Linux 2023 base AMI matching the configured filter.
3. The provisioner pipeline runs the numbered scripts in order.
4. **`90-marketplace-finalise.sh`** (LAST) scrubs SSH host keys, ec2-user
   `authorized_keys`, history files, journal, and dnf cache, then stamps
   `/etc/aws-marketplace/productcode`.
5. Packer registers the AMI, snapshots the EBS volume, and tags both with the
   Marketplace metadata (`ProductCode`, `Version`, `Architecture`, ...).

## rdkafka / glibc caveat (READ THIS before building)

FerroStash's **default** build pulls `rdkafka`, which **vendors librdkafka and
compiles it with CMake + a C toolchain**. Two consequences:

- **The cross image needs cmake + a C/clang toolchain.** `Cross.toml`'s
  `pre-build` hooks `apt-get install cmake clang pkg-config` into the target
  image. Connector TLS is rustls, so no system OpenSSL is needed.
- **The GNU (glibc) target is used, not musl.** A musl-static build of the
  vendored librdkafka is far more fragile. The resulting binary is therefore
  **dynamically linked against glibc**, so the cross image's glibc must be
  `<=` Amazon Linux 2023's (`~2.34`). `cross` 0.2.5+ default GNU images are
  Ubuntu 20.04 (glibc 2.31), which satisfies this. If you bump `cross` or pin a
  different image, keep the glibc old enough or the service fails at launch with
  a `GLIBC_2.xx not found` loader error. (If you ever must go fully static,
  building natively on an AL2023 arm64 instance is the safer alternative to a
  musl cross of librdkafka.)

The optional `ruby` (Artichoke/mruby) feature is **excluded** from the default
build, so no extra C++/mruby toolchain is required.

## Quickstart -- real build (arm64)

```bash
# 1. Install Packer 1.10+, Docker, and the `cross` tool; configure AWS creds.
cargo install cross   # if not present
aws configure         # or `aws sso login`

# 2. Build arm64
bash marketplace/packer/build.sh \
    --arch arm64 \
    --var "ferrostash_version=1.0.0" \
    --var "marketplace_product_code=prod-XXXXXXXXXXXXX" \
    --var "encrypt_boot_volume=false"

# 3. Inspect the produced AMI IDs
jq . marketplace/packer/manifest.json
```

## Quickstart -- offline dry-run (no AWS, no cargo)

```bash
bash marketplace/packer/build.sh --dry-run
```

The dry-run runs `tests/structure.sh` and `tests/lint.sh` only. `packer fmt
--check`, `packer validate -syntax-only`, `shellcheck`, and `bash -n` are
honoured when present and silently skipped when not. `bash -n` always runs.

## AWS permissions

The credentials Packer runs under need:

| Action group | Permissions |
|---|---|
| EC2 | `RunInstances`, `TerminateInstances`, `DescribeInstances`, `DescribeImages`, `DescribeSubnets`, `DescribeSecurityGroups`, `CreateTags`, `CreateImage`, `RegisterImage`, `DeregisterImage`, `ModifyImageAttribute`, `DescribeVolumes`, `AttachVolume`, `DetachVolume`, `DeleteSnapshot`, `CreateSnapshot`, `DescribeSnapshots`, `CopySnapshot`, `CopyImage` |
| EBS | `CreateVolume`, `DeleteVolume` |
| IAM | `PassRole` (only if you use an instance profile) |

## Marketplace submission checklist (enforced here)

| Requirement | Where it is enforced |
|---|---|
| No default passwords / no admin login | FerroStash has no auth surface; the service runs unprivileged. |
| No baked-in plaintext credentials | `30-install-config.sh` greps the pipeline + refuses to ship if a real key is found. |
| Root SSH login disabled | `60-harden.sh` writes `/etc/ssh/sshd_config.d/00-ferro-stash-hardening.conf` with `PermitRootLogin no`. |
| Password auth disabled | Same drop-in: `PasswordAuthentication no`, `KbdInteractiveAuthentication no`. |
| AWS Marketplace product code present | `90-marketplace-finalise.sh` writes `/etc/aws-marketplace/productcode`. |
| No baked SSH host keys | `90-marketplace-finalise.sh` removes them; AL2023's `sshd-keygen.service` regenerates on first boot. |
| No baked `authorized_keys` for default user | `90-marketplace-finalise.sh` removes `ec2-user` + `root` `authorized_keys`. |
| Bash history / journal / audit scrubbed | `90-marketplace-finalise.sh`. |
| cloud-init seed cleared | `90-marketplace-finalise.sh` runs `cloud-init clean --logs --seed`. |
| Up-to-date security errata | `00-install-deps.sh` runs `dnf -y --security upgrade`. |
| SELinux enforcing | `60-harden.sh` flips `/etc/selinux/config` + schedules `/.autorelabel`. |
| fail2ban watching sshd | `60-harden.sh` writes `/etc/fail2ban/jail.d/00-ferro-stash.local`. |
| Automatic security updates | `60-harden.sh` enables `dnf-automatic.timer` with `upgrade_type = security`. |
| Time sync (metering precondition) | `00-install-deps.sh` enables `chronyd`. |
| systemd hardening for the service | `files/ferro-stash.service` (`Type=simple`, `ProtectSystem=strict`, `NoNewPrivileges=true`, ...). |

## Reproducibility caveats

* **Base AMI rotates monthly.** The default filter `al2023-ami-2023.*-<arch>`
  resolves to the most recent published base AMI. Pin a specific AMI ID via
  `-var "base_ami_filter_arm64=al2023-ami-2023.x.YYYYMMDD.0-*-arm64"` for a fully
  reproducible build.
* **Timestamps in `local.build_suffix`.** Set `-var "build_id=<git-sha>"` for
  deterministic AMI names.
* **dnf updates.** `00-install-deps.sh` calls `dnf upgrade`, which pulls
  whatever errata are current at build time. Pair with a pinned base-AMI ID for
  a fully reproducible AMI.

## Deferrals (post-launch)

* **Signed AMIs / cosign attestations on the binary.**
* **EBS volume encryption with a customer-managed KMS CMK** (the current build
  supports the AWS-managed key via `encrypt_boot_volume`; CMK BYOK is later).
* **x86_64 listing** (the build supports it; the initial product is arm64).
