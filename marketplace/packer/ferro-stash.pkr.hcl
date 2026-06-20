# SPDX-License-Identifier: Apache-2.0
# FerroStash AWS Marketplace AMI -- multi-architecture Packer build.
#
# Two builders share the same provisioner pipeline so the arm64 and
# x86_64 AMIs are bit-identical modulo the source binary and the
# pinned base AMI ID.
#
# Usage (real build, arm64 only -- the initial Marketplace product):
#
#   packer init  marketplace/packer
#   packer build \
#     -only="ferro-stash-marketplace.amazon-ebs.arm64" \
#     -var "ferrostash_version=1.0.0" \
#     -var "marketplace_product_code=prod-XXXXXXXXXXXXX" \
#     -var "source_binary_dir=$PWD/marketplace/packer/build" \
#     -var "encrypt_boot_volume=false" \
#     marketplace/packer
#
# Usage (offline lint):
#
#   bash marketplace/packer/build.sh --dry-run

packer {
  required_version = ">= 1.10.0"
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = ">= 1.3.0"
    }
  }
}

# ---------------------------------------------------------------------------
# Local values
# ---------------------------------------------------------------------------

locals {
  # Stable suffix used in the AMI name. `build_id` lets CI inject a
  # reproducible identifier (e.g. the git SHA); when empty we fall back
  # to Packer's epoch timestamp so two manual builds never collide.
  build_suffix = var.build_id != "" ? var.build_id : formatdate("YYYYMMDDhhmmss", timestamp())

  ami_name_arm64  = "${var.ami_name_prefix}-${var.ferrostash_version}-arm64-${local.build_suffix}"
  ami_name_x86_64 = "${var.ami_name_prefix}-${var.ferrostash_version}-x86_64-${local.build_suffix}"

  # Tags applied to the produced AMI + EBS snapshot. Marketplace
  # validation tooling reads `Name`, `Architecture`, `Version`, and
  # `ProductCode`.
  common_tags = {
    Name         = var.ami_name_prefix
    Project      = "FerroStash"
    Version      = var.ferrostash_version
    Vendor       = "abyo software LLC"
    License      = "Apache-2.0"
    ProductCode  = var.marketplace_product_code
    BaseAmiOwner = var.base_ami_owner
    BuildTool    = "packer"
    BuildSuffix  = local.build_suffix
  }
}

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

source "amazon-ebs" "arm64" {
  region          = var.region
  instance_type   = var.instance_type_arm64
  ssh_username    = var.ssh_username
  ami_name        = local.ami_name_arm64
  ami_description = var.ami_description
  encrypt_boot    = var.encrypt_boot_volume
  skip_create_ami = var.skip_create_ami

  source_ami_filter {
    filters = {
      name                = var.base_ami_filter_arm64
      virtualization-type = "hvm"
      architecture        = "arm64"
      root-device-type    = "ebs"
    }
    most_recent = true
    owners      = [var.base_ami_owner]
  }

  # 8 GiB is the smallest size that fits Amazon Linux 2023 + the
  # FerroStash binary + persistent-queue / output dir headroom while
  # remaining cheap on Marketplace per-hour billing. Buyers can grow
  # the volume at launch.
  launch_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = 8
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = var.encrypt_boot_volume
  }

  ami_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = 8
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = var.encrypt_boot_volume
  }

  tags          = merge(local.common_tags, { Architecture = "arm64" })
  snapshot_tags = merge(local.common_tags, { Architecture = "arm64" })
  run_tags      = merge(local.common_tags, { Role = "packer-builder", Architecture = "arm64" })
}

source "amazon-ebs" "x86_64" {
  region          = var.region
  instance_type   = var.instance_type_x86_64
  ssh_username    = var.ssh_username
  ami_name        = local.ami_name_x86_64
  ami_description = var.ami_description
  encrypt_boot    = var.encrypt_boot_volume
  skip_create_ami = var.skip_create_ami

  source_ami_filter {
    filters = {
      name                = var.base_ami_filter_x86_64
      virtualization-type = "hvm"
      architecture        = "x86_64"
      root-device-type    = "ebs"
    }
    most_recent = true
    owners      = [var.base_ami_owner]
  }

  launch_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = 8
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = var.encrypt_boot_volume
  }

  ami_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = 8
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = var.encrypt_boot_volume
  }

  tags          = merge(local.common_tags, { Architecture = "x86_64" })
  snapshot_tags = merge(local.common_tags, { Architecture = "x86_64" })
  run_tags      = merge(local.common_tags, { Role = "packer-builder", Architecture = "x86_64" })
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build {
  name = "ferro-stash-marketplace"

  sources = [
    "source.amazon-ebs.arm64",
    "source.amazon-ebs.x86_64",
  ]

  # ---- Stage 0: install OS dependencies ---------------------------------
  provisioner "shell" {
    name            = "00-install-deps"
    script          = "${path.root}/scripts/00-install-deps.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Stage 1: create the ferrostash system user -----------------------
  provisioner "shell" {
    name            = "10-create-ferrostash-user"
    script          = "${path.root}/scripts/10-create-ferrostash-user.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Stage 2: upload the locally built binary -------------------------
  #
  # The architecture-specific binary subdir is selected by `source.name`
  # (the short label assigned to each builder above). `source_binary_dir`
  # is supplied by the operator via -var; the `build.sh` wrapper produces
  # it. Transfer uses the SFTP-backed `provisioner "file"`; the staging
  # dir on the build VM is pre-created in scripts/00 so the upload never
  # races a missing directory.
  provisioner "file" {
    name        = "upload-binary"
    source      = "${var.source_binary_dir}/${source.name}/ferro-stash"
    destination = "/tmp/ferro-stash"
  }

  provisioner "shell" {
    name            = "20-install-binaries"
    script          = "${path.root}/scripts/20-install-binaries.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Stage 3: install the default pipeline config ---------------------
  provisioner "file" {
    name        = "upload-pipeline-config"
    source      = "${path.root}/files/pipeline.conf"
    destination = "/tmp/pipeline.conf"
  }
  provisioner "shell" {
    name            = "30-install-config"
    script          = "${path.root}/scripts/30-install-config.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Stage 4: install the systemd units ------------------------------
  provisioner "file" {
    name        = "upload-systemd-ferro-stash-service"
    source      = "${path.root}/files/ferro-stash.service"
    destination = "/tmp/ferro-stash.service"
  }
  provisioner "file" {
    name        = "upload-systemd-firstboot-service"
    source      = "${path.root}/files/firstboot-systemd.service"
    destination = "/tmp/firstboot-systemd.service"
  }
  provisioner "file" {
    name        = "upload-firstboot-script"
    source      = "${path.root}/scripts/50-firstboot.sh"
    destination = "/tmp/50-firstboot.sh"
  }
  provisioner "shell" {
    name            = "40-install-systemd-units"
    script          = "${path.root}/scripts/40-install-systemd-units.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Stage 5: harden the OS (SELinux / fail2ban / dnf-automatic /
  #                              sshd) ----------------------------------
  provisioner "shell" {
    name            = "60-harden"
    script          = "${path.root}/scripts/60-harden.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Stage 6: Marketplace finalise (productcode + history scrub +
  #               regenerate-on-boot SSH host keys + SSH-key guard) ------
  #
  # This MUST run LAST: it removes ec2-user/root authorized_keys and the
  # SSH host keys, so any later provisioner that needs the SSH session
  # would break. It is the final stage in the pipeline by design.
  provisioner "shell" {
    name = "90-marketplace-finalise"
    environment_vars = [
      "FERROSTASH_PRODUCT_CODE=${var.marketplace_product_code}",
      "FERROSTASH_VERSION=${var.ferrostash_version}",
    ]
    script          = "${path.root}/scripts/90-marketplace-finalise.sh"
    execute_command = "sudo -E -S bash '{{ .Path }}'"
  }

  # ---- Manifest -------------------------------------------------------
  post-processor "manifest" {
    output     = "${path.root}/manifest.json"
    strip_path = true
    custom_data = {
      ferrostash_version = var.ferrostash_version
      product_code       = var.marketplace_product_code
      build_suffix       = local.build_suffix
    }
  }
}
