# Deepwater 24/7 Production Setup

## Prerequisites
- Python 3.13+
- Virtual environment activated

## Installation

```bash
pip install -r requirements.txt
```

## Setup for 24/7 Operation

### 1. Health Check
Verify platform is operational:

```bash
cd src
python -m deepwater.health_check --base-path ../data/coinbase-test --check-feeds --max-age-seconds 300
```

Exit codes:
- `0` = healthy
- `1` = unhealthy (see output for details)
- `2` = error running check

### 2. Automatic Cleanup (Cron)
Install retention cleanup to run every 15 minutes:

```bash
cd src
python -m deepwater.cleanup --install-cron --base-path ../data/coinbase-test --interval 15
```

Verify installation:
```bash
crontab -l | grep deepwater
```

Uninstall:
```bash
python -m deepwater.cleanup --uninstall-cron
```

Manual cleanup (dry run first):
```bash
python -m deepwater.cleanup --base-path ../data/coinbase-test --dry-run
python -m deepwater.cleanup --base-path ../data/coinbase-test
```

### 3. Version Enforcement
The platform enforces version compatibility via `manifest.json`. If you see:
```
RuntimeError: Deepwater version mismatch
```

**Options:**
1. Use the correct Deepwater version that created the data
2. Delete `data/coinbase-test/manifest.json` to re-init (may break compatibility)
3. Migrate data (future feature)

## Monitoring

### Health Check in Production
Add to your monitoring system (nagios/prometheus/etc):

```bash
*/5 * * * * cd /path/to/deepwater/src && python -m deepwater.health_check --base-path ../data/coinbase-test --check-feeds --max-age-seconds 300 || echo "Deepwater unhealthy" | mail -s "Alert" admin@example.com
```

### Log Files
- **Platform logs**: `data/coinbase-test/deepwater.log` (rotates at 10MB, keeps 5 backups)
- **Headless ingest**: `data/coinbase-test/headless.log`

### Test Applications

**Live ingest** (headless, AI-friendly):
```bash
cd src
python tests/headless_websocket.py
```

**Read recorded data**:
```bash
cd src
python tests/headless_reader.py
```

**Corruption detection/repair** (use with caution):
```bash
cd src
python tests/headless_repair.py
```

All headless tools use `print()` and `input()` for easy AI/script interaction.

## Production Checklist

- ✅ Logging configured (auto-rotate)
- ✅ Manifest version enforcement (blocks incompatible versions)
- ✅ Graceful shutdown (SIGTERM, SIGINT, atexit)
- ✅ Automatic retention cleanup (cron)
- ✅ Health check script (for monitoring)
- ✅ Headless test tools (AI-compatible)
- ⏳ Repair logic (EXISTS but untested - use cautiously)
- ⏳ Systemd service (application-level - user's responsibility)
- ⏳ Data migration tools (future)

## Breaking Changes

Version 0.0.1 removes `rotate_s` from GlobalRegistry binary format. Data created with pre-0.0.1 versions is incompatible. The manifest enforcement will prevent accidental corruption.
