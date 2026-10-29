# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- GitHub Actions workflows for pull request builds, CLA checks and releases, using the shared BonfireTrunk workflows.
- `CHANGELOG.md` and a Contributing section in the README.

### Changed

- Parent POM moved to bonfire-oss-parent 2.1.1. Lombok is now declared by this project, and all sources are formatted with palantir-java-format through Spotless, which CI enforces.
- The Redis integration test now starts its own Redis with Testcontainers and runs under maven-failsafe-plugin in the `integration-test` phase. It no longer expects a Redis on a fixed local port.
- Maven Wrapper upgraded to 3.3.4 with Maven 3.10.0 and a pinned distribution checksum.
