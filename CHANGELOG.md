# Changelog

All notable changes to Deepwater will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1] - 2026-02-01

### Added
- Initial release of Deepwater
- Zero-copy shared memory substrate with microsecond precision
- Uniform format (UF) record layouts with automatic struct generation
- Platform API for creating feeds, writers, and readers
- Time-range queries with binary search on timestamp indices
- Ring buffer support for transient feeds (persist=False)
- Global registry and per-feed registries (mmap-backed)
- Automatic retention management with cron support
- Health check CLI tool (`deepwater-health`)
- Cleanup CLI tool with cron installation (`deepwater-cleanup`)
- Repair CLI tool for corruption detection (`deepwater-repair`)
- Manifest-based version enforcement (prevents data corruption)
- Graceful shutdown handlers (SIGTERM, SIGINT, atexit)
- Auto-rotating logs (10MB max, 5 backups)
- Headless test applications for AI/script interaction
- TUI dashboard with prompt_toolkit (example)
- WebSocket market data ingestion example (Coinbase)

### Changed
- N/A (initial release)

### Breaking
- GlobalRegistry binary format: Removed `rotate_s` field (breaking change from pre-release versions)
- Manifest enforcement: Will block operations if version mismatch detected

### Fixed
- N/A (initial release)

### Security
- N/A (initial release)

---

## Release Process

1. Update version in `src/deepwater/__init__.py` and `pyproject.toml`
2. Update this CHANGELOG with changes
3. Run tests and verify examples
4. Build: `python -m build`
5. Test locally: `pip install dist/deepwater-X.Y.Z-py3-none-any.whl`
6. Commit: `git commit -m "Release vX.Y.Z"`
7. Tag: `git tag vX.Y.Z`
8. Push: `git push origin main --tags`
9. Publish: `twine upload dist/deepwater-X.Y.Z*`
10. Create GitHub release with artifacts

---

[Unreleased]: https://github.com/DanielGove/Deepwater/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/DanielGove/Deepwater/releases/tag/v0.0.1
