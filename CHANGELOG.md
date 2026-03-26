# Changelog

## v1.0.1 — 2026-03-26

### Fixed
- `get_client()`: now tries `garth.load()` + profile validation first before falling back to email/password login. This prevents hammering the Garmin SSO endpoint on every run, which was causing 429 Too Many Requests errors for users running the script frequently (e.g. via cron).

## v1.0.0 — 2026-03-17

### Added
- Full Garmin Connect health data fetching (40+ metrics)
- Support for 14 data categories: daily summary, HR, sleep, stress, body battery, SpO2, respiration, HRV, training status/readiness, VO2 max, endurance, race predictions, weight/body composition, activities, weekly steps
- Multiple credential methods: CLI args, env vars, macOS Keychain, credentials file
- JSON caching per day + latest snapshot
- English UI with structured JSON output
- Cross-platform compatibility
