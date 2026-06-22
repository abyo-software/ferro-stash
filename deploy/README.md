<!-- SPDX-License-Identifier: Apache-2.0 -->
# FerroStash deployment

Production packaging for the `ferro-stash` binary (a Logstash-compatible data
pipeline). Three delivery methods are provided:

- **Docker** — `../Dockerfile`
- **systemd** — `systemd/ferro-stash.service`
- **Helm** — `helm/ferro-stash/`

## Confirmed facts (verified against source)

| Thing | Value |
|-------|-------|
| Binary name | `ferro-stash` (crate `ferro-stash-cli`) |
| Config flag | `-f` / `--path.config` (alias `--config`) |
| Inline config | `-e` / `--config.string` |
| Data dir flag | `--path.data` (default `data`, relative to CWD) |
| API/metrics port | **9600** (`--api.http.host`, default `127.0.0.1:9600`) |
| Health endpoint | `GET /_health_report` (also `/_node`, `/_node/stats`) |
| Version / help | `--version` / `-V`, `--help` / `-h` |

The monitoring API defaults to **127.0.0.1**. Read-only stats endpoints are unauthenticated for Logstash compatibility; runtime log-level changes via `PUT /_node/logging` are disabled unless `--api.runtime_logging.enabled=true` is set. Expose port 9600 only through a trusted tunnel, reverse proxy, port-forward, or private cluster network.

`--path.data` holds a per-instance lock file, the instance `uuid`, and any
persistent queue / DLQ state. It must be unique per instance and writable.

## The Artichoke build dependency (optional `ruby` feature)

The `ruby` filter is **optional and off by default**. `ferro-stash-ruby`
depends on a fork of Artichoke pulled as a **rev-pinned git dependency**:

```toml
# crates/ferro-stash-ruby/Cargo.toml
artichoke-backend = { git = "https://github.com/abyo-software/artichoke-extended", rev = "245b894...", ... }
```

The fork is public at <https://github.com/abyo-software/artichoke-extended>
(branch `extended`). Because it is a git dependency recorded in `Cargo.lock`:

- **A fresh clone builds with no extra checkout** — `cargo build --features
  ruby` fetches the pinned revision automatically. The default `cargo build`
  doesn't fetch it at all.
- **CI / the release workflow / Docker** build with `--features ruby` and rely
  on cargo to fetch the fork; there is no sibling-checkout or repo-nesting hack.
- The repo-root Docker image ships **with** the `ruby` filter enabled for
  migration compatibility. Marketplace artifacts use the default no-Ruby build.

Build toolchain needed: Rust stable, **cmake** (vendored librdkafka for the
kafka plugins). The `ruby` feature additionally needs **clang/gcc** (mruby FFI)
— not required for the default build. TLS uses rustls — no system OpenSSL
required. At runtime only `ca-certificates` is needed for outbound TLS.

---

## Docker

```bash
# Build (slow first time: compiles rdkafka, aws-sdk, mruby).
docker build -t ferro-stash:latest .

# Run with a mounted pipeline config.
docker run --rm -it \
  -v "$PWD/config/example.conf:/etc/ferro-stash/pipeline.conf:ro" \
  ferro-stash:latest

# To publish the monitoring API deliberately, bind all interfaces.
docker run --rm -it \
  -v "$PWD/config/example.conf:/etc/ferro-stash/pipeline.conf:ro" \
  -p 9600:9600 \
  ferro-stash:latest -f /etc/ferro-stash/pipeline.conf --path.data /var/lib/ferro-stash --api.http.host 0.0.0.0:9600

# Check the API.
curl http://127.0.0.1:9600/_node
curl http://127.0.0.1:9600/_health_report
```

The image:
- runs as non-root uid/gid `65532`,
- ships `ca-certificates` + `tini` (signal forwarding / zombie reaping),
- uses the writable volume `/var/lib/ferro-stash` as `path.data`,
- binds the API on `127.0.0.1:9600` by default,
- default `CMD` reads `/etc/ferro-stash/pipeline.conf`.

### Optional GeoIP database

The `geoip` filter reads a user-supplied MaxMind `.mmdb` at runtime (not
vendored). Mount it and point the filter at it:

```bash
docker run --rm -it \
  -v "$PWD/pipeline.conf:/etc/ferro-stash/pipeline.conf:ro" \
  -v "$PWD/GeoLite2-City.mmdb:/etc/ferro-stash/GeoLite2-City.mmdb:ro" \
  -p 9600:9600 ferro-stash:latest
```

```conf
filter {
  geoip {
    source   => "client_ip"
    database => "/etc/ferro-stash/GeoLite2-City.mmdb"
  }
}
```

---

## systemd

```bash
# Binary + config.
sudo install -m0755 target/release/ferro-stash /usr/local/bin/ferro-stash
sudo install -d -m0755 /etc/ferro-stash
sudo install -m0644 config/example.conf /etc/ferro-stash/pipeline.conf

# Service account (non-root).
sudo useradd --system --no-create-home --shell /usr/sbin/nologin ferrostash

# Unit.
sudo install -m0644 deploy/systemd/ferro-stash.service \
     /etc/systemd/system/ferro-stash.service
sudo systemctl daemon-reload
sudo systemctl enable --now ferro-stash

# Logs / status.
systemctl status ferro-stash
journalctl -u ferro-stash -f
```

The unit:
- runs as `ferrostash:ferrostash`,
- uses `StateDirectory=ferro-stash` (`/var/lib/ferro-stash`, mode 0750) as
  `path.data`,
- binds the API on `127.0.0.1:9600` (front it with a reverse proxy if remote
  access is needed),
- is hardened: `ProtectSystem=strict`, `NoNewPrivileges`, `PrivateTmp`,
  `ProtectHome`, restricted syscalls/address-families, with only
  `/var/lib/ferro-stash` writable (`ReadWritePaths`).

For GeoIP, place the database at `/etc/ferro-stash/GeoLite2-City.mmdb` (the
config directory is read-only to the service, which is correct — the db is read
only) and reference it from `pipeline.conf` as above.

> Note: `MemoryDenyWriteExecute` is intentionally left `false` — the `script`
> filter uses a Cranelift JIT that needs writable+executable memory.

---

## Helm

```bash
# Lint.
helm lint deploy/helm/ferro-stash

# Install (set your image + pipeline).
helm install fs deploy/helm/ferro-stash \
  --set image.repository=myrepo/ferro-stash \
  --set image.tag=1.0.0 \
  --set-file pipelineConf=./my-pipeline.conf
```

Key values (`deploy/helm/ferro-stash/values.yaml`):

| Value | Purpose |
|-------|---------|
| `image.repository` / `image.tag` | container image (tag defaults to chart appVersion) |
| `pipelineConf` | the Logstash `.conf` pipeline; rendered into a ConfigMap and mounted at `/etc/ferro-stash/pipeline.conf`; changes roll the pods (checksum annotation) |
| `service.port` | API/metrics port (default 9600) |
| `resources` | container requests/limits |
| `persistence.enabled` | use a PVC for `path.data` instead of `emptyDir` |
| `geoip.enabled` + `geoip.existingSecret` | mount a `.mmdb` from a Secret |
| `probes.*` | liveness/readiness against `/_health_report` |

The pod runs non-root (uid 65532) with `readOnlyRootFilesystem: true`; the only
writable mount is the `path.data` volume at `/var/lib/ferro-stash`.

### Providing the GeoIP database

```bash
kubectl create secret generic ferro-stash-geoip \
  --from-file=GeoLite2-City.mmdb=./GeoLite2-City.mmdb

helm upgrade fs deploy/helm/ferro-stash \
  --set geoip.enabled=true \
  --set geoip.existingSecret=ferro-stash-geoip
```

The database mounts at `/etc/ferro-stash/GeoLite2-City.mmdb`; reference that
path in the `geoip` filter's `database =>` option.
