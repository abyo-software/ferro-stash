---
name: Bug report
about: Report a problem with FerroStash
title: ''
labels: bug
assignees: ''
---

**What happened**
A clear description of the bug.

**Pipeline config**
The relevant `pipeline.conf` (redact secrets). Note if it is a config that
Logstash runs differently.

**Expected vs actual**
What you expected, and what FerroStash did instead. If this is a Logstash
compatibility difference, include the Logstash output for the same input.

**Environment**
- FerroStash version (`ferro-stash --version`) or commit:
- Install method (cargo build / docker / AWS Marketplace AMI / container):
- OS / arch:
- Built with `--features ruby`? (yes/no)

**Logs**
Relevant output (run with `RUST_LOG=debug` if possible).
