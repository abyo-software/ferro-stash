# SPDX-License-Identifier: Apache-2.0
# FerroStash AWS Marketplace AMI -- typed Packer variables.
#
# Splitting the variable declarations into a dedicated file keeps the
# build (`ferro-stash.pkr.hcl`) focused on builder + provisioner wiring
# and lets operators override values via `-var` / `*.pkrvars.hcl` without
# editing the build file.

variable "region" {
  type        = string
  description = "AWS region the AMI is built in. Marketplace listings later copy the AMI to additional regions."
  default     = "us-east-1"
}

variable "ami_name_prefix" {
  type        = string
  description = "Prefix used for the produced AMI name. The build appends architecture + ISO 8601 timestamp."
  default     = "ferro-stash"
  validation {
    # Marketplace AMI names must match `[A-Za-z0-9().,/_-]{3,128}`. We
    # restrict the prefix tighter (no spaces, no uppercase) so the
    # composed name is always lower-snake-case.
    condition     = can(regex("^[a-z0-9][a-z0-9._-]{1,32}$", var.ami_name_prefix))
    error_message = "The ami_name_prefix must be 2-33 chars, lowercase alphanumeric / dot / underscore / hyphen."
  }
}

variable "ami_description" {
  type        = string
  description = "AMI description shown in the EC2 console and in the Marketplace listing. Plain ASCII (no em-dash / curly quotes)."
  default     = "FerroStash -- Rust-native, Logstash-compatible log and event pipeline (Apache-2.0)."
}

variable "instance_type_arm64" {
  type        = string
  description = "EC2 instance type used for the arm64 (Graviton) build VM."
  default     = "c7g.large"
}

variable "instance_type_x86_64" {
  type        = string
  description = "EC2 instance type used for the x86_64 build VM."
  default     = "c7i.large"
}

variable "base_ami_owner" {
  type        = string
  description = "Owner alias of the base AMI. `amazon` resolves to the official Amazon Linux 2023 AMI publisher."
  default     = "amazon"
}

variable "base_ami_filter_arm64" {
  type        = string
  description = "Name filter for the arm64 Amazon Linux 2023 base AMI. The filter pins to the kernel-default LTS lineage; the most recent match is used."
  default     = "al2023-ami-2023.*-arm64"
}

variable "base_ami_filter_x86_64" {
  type        = string
  description = "Name filter for the x86_64 Amazon Linux 2023 base AMI."
  default     = "al2023-ami-2023.*-x86_64"
}

variable "ssh_username" {
  type        = string
  description = "Login user on the Amazon Linux 2023 base AMI."
  default     = "ec2-user"
}

variable "source_binary_dir" {
  type        = string
  description = "Directory containing the locally built FerroStash binary. Subdirectories `arm64/` and `x86_64/` must each contain `ferro-stash`."
  default     = "./build"
}

variable "marketplace_product_code" {
  type        = string
  description = "AWS Marketplace product code. The placeholder `REPLACE-WITH-SELLER-PRODUCT-CODE` is intentional -- AWS issues the real code when the Marketplace listing is created. Replace via `-var marketplace_product_code=...` for the production build."
  default     = "REPLACE-WITH-SELLER-PRODUCT-CODE"
}

variable "ferrostash_version" {
  type        = string
  description = "Semantic version of FerroStash being baked into the AMI. Used for AMI tagging and the AMI name suffix."
  default     = "1.0.0"
}

variable "build_id" {
  type        = string
  description = "Optional opaque build identifier appended to the AMI name. When unset, Packer's `{{timestamp}}` is used."
  default     = ""
}

variable "encrypt_boot_volume" {
  type        = bool
  description = "Whether the produced AMI's root EBS snapshot is encrypted with the AWS-managed key. Marketplace AMIs MUST leave this off (an encrypted boot snapshot cannot be cross-account shared with the ingestion role); flip to `true` only for internal/private fleets."
  default     = false
}

variable "skip_create_ami" {
  type        = bool
  description = "When `true`, Packer runs the build but does not register the AMI. Useful for CI smoke tests."
  default     = false
}
