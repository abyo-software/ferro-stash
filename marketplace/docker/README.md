# FerroStash -- AWS Marketplace container image

This directory builds the **paid, metered container image** published to the AWS
Marketplace `ContainerProduct@1.0` listing. It is separate from the repo-root
`./Dockerfile` (the OSS image): this one is built `--features marketplace` so the
binary calls AWS Marketplace Metering `RegisterUsage` once at startup as an
entitlement gate, and it omits the optional `ruby` filter (matching the published
honest scope).

## Single-manifest requirement (critical)

The AWS Marketplace container scanner **rejects** an OCI image *index* that
carries provenance/SBOM attestation children with a `SCAN_ERROR`
(`UnsupportedImageType`). That index is what Docker 24+/buildx produces **by
default**. The pushed image MUST be a single image manifest
(`application/vnd.docker.distribution.manifest.v2+json`).

Always build with attestations OFF and a single platform, from the **repo root**
(the build context is the whole workspace):

```bash
TAG=709825985650.dkr.ecr.us-east-1.amazonaws.com/abyo-software/ferro-stash-container:1.0.2
docker build \
  --provenance=false --sbom=false --platform linux/amd64 \
  --build-arg FERROSTASH_BUILD_SHA="$(git rev-parse --short=12 HEAD)" \
  --build-arg FERROSTASH_BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -f marketplace/docker/Dockerfile.marketplace \
  -t "$TAG" .
```

The two `--build-arg`s feed the build script (`crates/ferro-stash-cli/build.rs`)
so the monitoring API and `--version` line carry the actual commit + build
timestamp, not the `unknown` placeholder. `.git/` is `.dockerignore`d (its size
inflates the build context), so without these the in-container build cannot
shell out to git.

Verify it is a single manifest (NOT an index) before pushing:

```bash
docker buildx imagetools inspect "$TAG" --format '{{.Manifest.MediaType}}'
# must print: application/vnd.docker.distribution.manifest.v2+json
# (NOT application/vnd.oci.image.index.v1+json)
```

## Push (operator step; tags are IMMUTABLE)

The Marketplace ECR repo is provisioned by `deploy/marketplace-container.sh`
(`AddRepositories`). ECR tags in that repo are **immutable** -- a wrong first
push cannot be overwritten or deleted; bump the tag instead.

```bash
aws --profile as ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin 709825985650.dkr.ecr.us-east-1.amazonaws.com
docker push "$TAG"
```

## Image facts

| Aspect | Value |
|--------|-------|
| Architecture | linux/amd64, single manifest |
| Builder base | `rust:1.95-bookworm` |
| Runtime base | `debian:bookworm-slim` (+ `ca-certificates`, `tini`) |
| User | non-root `65532:65532` |
| Default config | `/etc/ferro-stash/pipeline.conf` (Beats in :5044 -> file out) |
| State volume | `/var/lib/ferro-stash` |
| Ports | 9600 (monitoring API), 5044 (default Beats input) |
| Feature flags | `marketplace` ON, `ruby` OFF |
| Entitlement env | `FERROSTASH_MARKETPLACE_PRODUCT_CODE` (unset => gate skipped) |

## Entitlement behaviour

At startup the binary reads `FERROSTASH_MARKETPLACE_PRODUCT_CODE` and resolves
the AWS region from the standard chain (`AWS_REGION` / profile / instance
metadata):

- env var **unset/blank** -> the check is skipped (dev / unmetered run).
- RegisterUsage **succeeds** -> entitled; the pipeline starts.
- **CustomerNotEntitled / InvalidProductCode / ...** -> logged and the process
  exits non-zero (fail closed).
- **transient** AWS/network error -> bounded retry, then exits non-zero (fail closed).

In EKS the pod needs AWS credentials (IRSA / Pod Identity) with permission to
call `aws-marketplace:RegisterUsage`, and the product code wired in via the Helm
chart `marketplace.productCode` value.
