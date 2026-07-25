# Changelog

All notable changes to the Orisun Go client will be documented in this file.

## Unreleased

- Added `GetServerInfo` for build, backend, node identity, and capability
  discovery.
- Added create, import, list, and get APIs for dynamic consistency boundaries.
- Regenerated protobuf bindings for the event-backed boundary catalog.
- Updated the module and CI/release toolchains to Go 1.26.5.

## v0.2.1 - 2026-06-05

- Rebuilt and validated the module with an updated Go toolchain to pick up
  standard library security fixes.

## v0.2.0 - 2026-06-05

- Prepared the module for public Go package indexing with package documentation
  and release metadata.
