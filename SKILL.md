# Garmin Connect Health Data Skill

Fetch comprehensive health & fitness data from Garmin Connect for your AI agent.

## Data Coverage

| Category | Fields |
|----------|--------|
| **Activity** | Steps, distance, calories (active + BMR), floors, intensity minutes |
| **Heart Rate** | Min/max/resting heart rate |
| **Sleep** | Duration, score, deep/light/REM/awake breakdown, stress during sleep |
| **HRV** | Last night avg, 5min peak, weekly avg, status (balanced/unbalanced), baseline |
| **Body Battery** | Current level, daily min/max |
| **SpO2** | Average and minimum blood oxygen |
| **Respiration** | Waking and sleep breathing rate |
| **Stress** | Average/max, rest/low/medium/high duration breakdown |
| **Training Status** | Overreaching/Highly Active/Productive/Maintaining/Recovery/Detraining + acute/chronic load ratio |
| **Training Readiness** | Score (0-100) |
| **Fitness Metrics** | VO2 Max, fitness age, endurance score, hill score |
| **Race Predictions** | 5K/10K/Half/Marathon predicted times |
| **Weight/Body Comp** | Weight (kg), body fat %, BMI (requires Garmin Index scale) |
| **Hydration** | Intake (ml) vs goal |
| **Activities** | Individual workouts with HR, duration, calories, elevation, training effect |
| **Weekly Summary** | Total/avg steps for the week |

## Setup

### 1. Install dependency
```bash
pip install garminconnect
```

### 2. Set credentials (choose one method)

**Option A — Environment variables:**
```bash
export GARMIN_EMAIL="you@example.com"
export GARMIN_PASSWORD="yourpassword"
```

**Option B — CLI args:**
```bash
python3 garmin_health.py --email you@example.com --password yourpassword
```

**Option C — macOS Keychain:**
```bash
security add-generic-password -a "you@example.com" -s "garmin_connect" -w "yourpassword"
```

**Option D — Credentials file:**
```bash
echo -e "email=you@example.com\npassword=yourpassword" > ~/.garmin_credentials
chmod 600 ~/.garmin_credentials
```

### 3. First run
First login may require MFA verification. You'll be prompted to enter a code sent to your email.

### 4. Use with OpenClaw
Ask your AI agent:
- "Show my health data"
- "How did I sleep last night?"
- "What's my HRV?"
- "Am I overtraining?"

## Usage

```bash
# Fetch today's data (default)
python3 garmin_health.py

# Fetch specific date
python3 garmin_health.py --date 2026-03-16

# Show latest cached data
python3 garmin_health.py --show

# With credentials
python3 garmin_health.py --email you@example.com --password pass
```

## Data Storage

- `~/.garmin_health/YYYY-MM-DD.json` — Daily snapshots
- `~/.garmin_health/latest.json` — Most recent fetch
- `~/.garminconnect/` — OAuth token cache

Override with env vars:
- `GARMIN_DATA_DIR` — Change data directory
- `GARMIN_TOKENSTORE` — Change token directory

## Supported Languages

All labels and output in English. JSON field names are English by design.

## Requirements

- Python 3.10+
- `garminconnect` library
- A Garmin Connect account
- Device: Any Garmin watch/fitness tracker synced to Garmin Connect
