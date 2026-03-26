#!/usr/bin/env python3
"""
Garmin Connect Full Health Data Fetcher — OpenClaw Skill
=========================================================
Fetches comprehensive health & fitness data from Garmin Connect
and saves it as structured JSON for your AI agent to query.

Usage:
  python3 garmin_health.py                        # Fetch today's data
  python3 garmin_health.py --date 2026-03-16      # Fetch specific date
  python3 garmin_health.py --show                 # Show latest cached data
  python3 garmin_health.py --email you@example.com --password yourpass

Auth (in order of priority):
  1. --email / --password CLI args
  2. GARMIN_EMAIL / GARMIN_PASSWORD environment variables
  3. macOS Keychain: security find-generic-password -s 'garmin_connect'
  4. ~/.garmin_credentials (format: email=..., password=...)

Data is saved to: ~/.garmin_health/YYYY-MM-DD.json
Latest snapshot:  ~/.garmin_health/latest.json
"""

import argparse
import json
import os
import sys
import subprocess
from datetime import date, timedelta, datetime

try:
    from garminconnect import Garmin
except ImportError:
    print("Please install: pip install garminconnect")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR   = os.environ.get("GARMIN_DATA_DIR",   os.path.expanduser("~/.garmin_health"))
TOKENSTORE = os.environ.get("GARMIN_TOKENSTORE", os.path.expanduser("~/.garminconnect"))


def _load_credentials(email_arg=None, password_arg=None):
    """Load credentials from args → env vars → macOS keychain → file."""
    email    = email_arg    or os.environ.get("GARMIN_EMAIL")
    password = password_arg or os.environ.get("GARMIN_PASSWORD")

    if not password:
        # macOS Keychain
        try:
            kc = subprocess.run(
                ["security", "find-generic-password", "-s", "garmin_connect", "-w"],
                capture_output=True, text=True
            )
            if kc.returncode == 0:
                password = kc.stdout.strip()
        except FileNotFoundError:
            pass  # Not macOS

    if not password:
        # Credentials file (~/.garmin_credentials should be chmod 600)
        cred_file = os.path.expanduser("~/.garmin_credentials")
        if os.path.exists(cred_file):
            # Warn if file permissions are too open
            import stat
            mode = os.stat(cred_file).st_mode
            if mode & (stat.S_IRWXG | stat.S_IRWXO):
                print(f"⚠️  Warning: {cred_file} is readable by others. Run: chmod 600 {cred_file}")
            for line in open(cred_file):
                k, _, v = line.strip().partition("=")
                if k == "email":    email    = v
                if k == "password": password = v

    if not email or not password:
        print("❌ Credentials not found. Provide via --email/--password, env vars")
        print("   GARMIN_EMAIL / GARMIN_PASSWORD, macOS Keychain, or ~/.garmin_credentials")
        sys.exit(1)

    return email, password


def get_client(email=None, password=None, is_cn=None):
    """Return an authenticated Garmin client.

    Strategy (mirrors the hardened local version):
      1. Try to load a cached garth token from TOKENSTORE — no network call.
      2. Validate the token by reading the profile; if valid, return immediately
         without ever touching the Garmin SSO endpoint.
      3. Only fall back to email/password login when the token is missing or
         expired.  After a successful fresh login the new token is persisted so
         subsequent calls are also cache-hits.

    This avoids hammering the SSO endpoint on every run (which triggers 429s).

    Region selection (in order of priority):
      --cn CLI flag  →  GARMIN_IS_CN=true env var  →  default: False (global)
    Set is_cn=True to use Garmin Connect CN (connect.garmin.com.cn), recommended
    for users with a Chinese Garmin account or when running from a mainland IP.
    """
    # Resolve is_cn: caller arg → env var → default False
    if is_cn is None:
        is_cn = os.environ.get("GARMIN_IS_CN", "").lower() in ("1", "true", "yes")

    # ── Step 1: attempt token-only auth ──────────────────────────────────────
    try:
        client = Garmin(is_cn=is_cn)
        client.garth.load(TOKENSTORE)
        profile = client.garth.profile
        client.display_name = profile.get("displayName") if profile else None
        if client.display_name:
            return client  # token valid — no SSO call needed
    except Exception:
        pass  # token missing / expired / corrupt → fall through to login

    # ── Step 2: fresh login with credentials ─────────────────────────────────
    email, password = _load_credentials(email, password)
    client = Garmin(email, password, is_cn=is_cn)
    try:
        client.login(TOKENSTORE)
    except Exception:
        client.login()
        client.garth.dump(TOKENSTORE)
    return client


def safe_get(fn, label, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        print(f"  ⚠️  {label}: {e}")
        return None


def fetch_all(target_date=None, email=None, password=None, is_cn=None):
    if not target_date:
        target_date = str(date.today())

    os.makedirs(DATA_DIR, exist_ok=True)
    cache_file = os.path.join(DATA_DIR, f"{target_date}.json")

    region = "CN (connect.garmin.com.cn)" if (is_cn or os.environ.get("GARMIN_IS_CN", "").lower() in ("1", "true", "yes")) else "Global (connect.garmin.com)"
    print(f"🔐 Connecting to Garmin Connect [{region}]...")
    client = get_client(email, password, is_cn=is_cn)
    print(f"✅ Connected. Fetching {target_date}...\n")

    result = {"date": target_date, "fetched_at": datetime.now().isoformat()}

    # ── Daily Summary ─────────────────────────
    print("📊 Daily summary...")
    daily = safe_get(client.get_stats, "stats", target_date)
    if daily:
        result.update({
            "steps":                      daily.get("totalSteps"),
            "calories":                   daily.get("totalKilocalories"),
            "active_calories":            daily.get("activeKilocalories"),
            "bmr_calories":               daily.get("bmrKilocalories"),
            "distance_meters":            daily.get("totalDistanceMeters"),
            "active_seconds":             daily.get("highlyActiveSeconds", 0),
            "sedentary_seconds":          daily.get("sedentarySeconds"),
            "resting_heart_rate":         daily.get("restingHeartRate"),
            "avg_stress":                 daily.get("averageStressLevel"),
            "max_stress":                 daily.get("maxStressLevel"),
            "floors_climbed":             daily.get("floorsAscended"),
            "floors_descended":           daily.get("floorsDescended"),
            "moderate_intensity_minutes": daily.get("moderateIntensityMinutes"),
            "vigorous_intensity_minutes": daily.get("vigorousIntensityMinutes"),
        })
        steps = result["steps"] or 0
        dist  = (result["distance_meters"] or 0) / 1000
        print(f"  👟 Steps: {steps:,}  📍 Distance: {dist:.2f} km")
        print(f"  🔥 Calories: {result['calories']} kcal  (active {result['active_calories']}, BMR {result['bmr_calories']})")
        print(f"  ⏱️  Moderate: {result['moderate_intensity_minutes']}min  Vigorous: {result['vigorous_intensity_minutes']}min")

    # ── Heart Rate ────────────────────────────
    print("❤️  Heart rate...")
    hr = safe_get(client.get_heart_rates, "heart_rates", target_date)
    if hr:
        result.update({
            "heart_rate_max":     hr.get("maxHeartRate"),
            "heart_rate_min":     hr.get("minHeartRate"),
            "heart_rate_resting": hr.get("restingHeartRate"),
        })
        print(f"  Range: {result['heart_rate_min']}-{result['heart_rate_max']} bpm  (resting {result['heart_rate_resting']})")

    # ── Sleep ─────────────────────────────────
    print("😴 Sleep...")
    sleep = safe_get(client.get_sleep_data, "sleep", target_date)
    if sleep and "dailySleepDTO" in sleep:
        dto = sleep["dailySleepDTO"]
        sleep_sec = dto.get("sleepTimeSeconds", 0)
        result.update({
            "sleep_hours":         round(sleep_sec / 3600, 1),
            "sleep_seconds":       sleep_sec,
            "sleep_score":         dto.get("sleepScores", {}).get("overall", {}).get("value"),
            "sleep_deep_seconds":  dto.get("deepSleepSeconds"),
            "sleep_light_seconds": dto.get("lightSleepSeconds"),
            "sleep_rem_seconds":   dto.get("remSleepSeconds"),
            "sleep_awake_seconds": dto.get("awakeSleepSeconds"),
            "avg_sleep_stress":    dto.get("avgSleepStress"),
        })
        deep  = (result["sleep_deep_seconds"]  or 0) // 60
        light = (result["sleep_light_seconds"] or 0) // 60
        rem   = (result["sleep_rem_seconds"]   or 0) // 60
        awake = (result["sleep_awake_seconds"] or 0) // 60
        print(f"  Duration: {result['sleep_hours']}h  Score: {result['sleep_score']}")
        print(f"  Deep: {deep}min  Light: {light}min  REM: {rem}min  Awake: {awake}min")

    # ── Stress ────────────────────────────────
    print("😰 Stress...")
    stress = safe_get(client.get_stress_data, "stress", target_date)
    if stress:
        result.update({
            "stress_avg":             stress.get("avgStressLevel"),
            "stress_max":             stress.get("maxStressLevel"),
            "rest_stress_duration":   stress.get("restStressDuration"),
            "low_stress_duration":    stress.get("lowStressDuration"),
            "medium_stress_duration": stress.get("mediumStressDuration"),
            "high_stress_duration":   stress.get("highStressDuration"),
        })
        print(f"  Avg: {result['stress_avg']}  Max: {result['stress_max']}")

    # ── Body Battery ──────────────────────────
    print("🔋 Body Battery...")
    bb_list = safe_get(client.get_body_battery, "body_battery", target_date, target_date)
    if bb_list and isinstance(bb_list, list):
        day_data = next((b for b in bb_list if isinstance(b, dict) and "bodyBatteryValuesArray" in b), None)
        if day_data:
            vals = [v[1] for v in day_data.get("bodyBatteryValuesArray", []) if v[1] is not None]
            if vals:
                result.update({
                    "body_battery_start": vals[0],
                    "body_battery_end":   vals[-1],
                    "body_battery_max":   max(vals),
                    "body_battery_min":   min(vals),
                })
                print(f"  Current: {result['body_battery_end']}  Range: {result['body_battery_min']}-{result['body_battery_max']}")

    # ── SpO2 ──────────────────────────────────
    print("🫁 SpO2...")
    spo2 = safe_get(client.get_spo2_data, "spo2", target_date)
    if spo2:
        result.update({
            "spo2_avg": spo2.get("averageSpO2"),
            "spo2_min": spo2.get("lowestSpO2"),
        })
        print(f"  Avg: {result['spo2_avg']}%  Min: {result['spo2_min']}%")

    # ── Respiration ───────────────────────────
    print("💨 Respiration...")
    resp = safe_get(client.get_respiration_data, "respiration", target_date)
    if resp:
        result.update({
            "respiration_avg_waking": resp.get("avgWakingRespirationValue"),
            "respiration_avg_sleep":  resp.get("avgSleepRespirationValue"),
        })
        print(f"  Waking: {result['respiration_avg_waking']} brpm  Sleep: {result['respiration_avg_sleep']} brpm")

    # ── HRV ───────────────────────────────────
    print("📈 HRV...")
    hrv = safe_get(client.get_hrv_data, "hrv", target_date)
    if hrv and "hrvSummary" in hrv:
        s  = hrv["hrvSummary"]
        bl = s.get("baseline", {}) or {}
        result.update({
            "hrv_weekly_avg":            s.get("weeklyAvg"),
            "hrv_last_night_avg":        s.get("lastNightAvg"),
            "hrv_last_night_5min_high":  s.get("lastNight5MinHigh"),
            "hrv_status":                s.get("status"),
            "hrv_feedback":              s.get("feedbackPhrase"),
            "hrv_baseline_balanced_low": bl.get("balancedLow"),
            "hrv_baseline_balanced_high":bl.get("balancedUpper"),
        })
        print(f"  Last night: {result['hrv_last_night_avg']} ms  5min peak: {result['hrv_last_night_5min_high']} ms  Weekly avg: {result['hrv_weekly_avg']} ms")
        print(f"  Status: {result['hrv_status']}  Baseline: {result['hrv_baseline_balanced_low']}-{result['hrv_baseline_balanced_high']} ms")

    # ── Training Readiness ────────────────────
    print("🎯 Training readiness...")
    readiness = safe_get(client.get_training_readiness, "training_readiness", target_date)
    if isinstance(readiness, list) and readiness:
        r = readiness[0] if isinstance(readiness[0], dict) else {}
        result["training_readiness_score"] = r.get("score")
    elif isinstance(readiness, dict):
        result["training_readiness_score"] = readiness.get("score")
    if result.get("training_readiness_score"):
        print(f"  Score: {result['training_readiness_score']}")

    # ── Training Status ───────────────────────
    print("📊 Training status...")
    tstatus = safe_get(client.get_training_status, "training_status", target_date)
    if isinstance(tstatus, dict):
        mrt       = tstatus.get("mostRecentTrainingStatus", {}) or {}
        latest    = mrt.get("latestTrainingStatusData", {}) or {}
        dev       = next(iter(latest.values()), {}) if latest else {}
        ts_code   = dev.get("trainingStatus")
        ts_labels = {1:"Overreaching", 2:"Highly Active", 3:"Productive", 4:"Maintaining",
                     5:"Recovery", 6:"Detraining", 7:"No Recent Activity"}
        acwr      = dev.get("acuteTrainingLoadDTO") or {}
        mlb       = tstatus.get("mostRecentTrainingLoadBalance", {}) or {}
        bmap      = mlb.get("metricsTrainingLoadBalanceDTOMap", {}) or {}
        bdata     = next(iter(bmap.values()), {}) if bmap else {}
        result.update({
            "training_status":                ts_labels.get(ts_code, str(ts_code) if ts_code else None),
            "training_status_code":           ts_code,
            "training_status_feedback":       dev.get("trainingStatusFeedbackPhrase"),
            "training_load_acute":            acwr.get("dailyTrainingLoadAcute"),
            "training_load_chronic":          acwr.get("dailyTrainingLoadChronic"),
            "training_load_ratio":            acwr.get("dailyAcuteChronicWorkloadRatio"),
            "training_load_ratio_status":     acwr.get("acwrStatus"),
            "training_load_balance_feedback": bdata.get("trainingBalanceFeedbackPhrase"),
        })
        print(f"  Status: {result['training_status']}  Acute: {result['training_load_acute']}  Chronic: {result['training_load_chronic']}  Ratio: {result['training_load_ratio']}")

    # ── Max Metrics (VO2 Max) ─────────────────
    print("🏆 Max metrics...")
    max_m = safe_get(client.get_max_metrics, "max_metrics", target_date)
    if isinstance(max_m, list) and max_m:
        m = max_m[0] if isinstance(max_m[0], dict) else {}
        g = m.get("generic") or {}
        result.update({
            "vo2max":      g.get("vo2MaxPreciseValue") or m.get("vo2MaxPreciseValue") or m.get("vo2MaxValue"),
            "fitness_age": g.get("fitnessAge") or m.get("fitnessAge"),
        })
        print(f"  VO2 Max: {result.get('vo2max')}  Fitness age: {result.get('fitness_age')}")

    # ── Endurance Score ───────────────────────
    print("💪 Endurance score...")
    end = safe_get(client.get_endurance_score, "endurance_score", target_date)
    if end:
        result["endurance_score"] = end.get("overallScore") if isinstance(end, dict) else (end if isinstance(end, (int, float)) else None)
        print(f"  Score: {result['endurance_score']}")

    # ── Race Predictions ──────────────────────
    print("🏃 Race predictions...")
    rp_list = safe_get(client.get_race_predictions, "race_predictions")
    if isinstance(rp_list, list) and rp_list:
        rp = rp_list[0] if isinstance(rp_list[0], dict) else {}
        def fmt(s): return f"{s//3600}:{(s%3600)//60:02d}:{s%60:02d}" if s else None
        result.update({
            "race_pred_5k":        fmt(rp.get("time5K")),
            "race_pred_10k":       fmt(rp.get("time10K")),
            "race_pred_half":      fmt(rp.get("timeHalfMarathon")),
            "race_pred_marathon":  fmt(rp.get("timeMarathon")),
        })
        print(f"  5K: {result['race_pred_5k']}  10K: {result['race_pred_10k']}  Half: {result['race_pred_half']}  Marathon: {result['race_pred_marathon']}")

    # ── Weight ────────────────────────────────
    print("⚖️  Weight...")
    wt = safe_get(client.get_weigh_ins, "weigh_ins", target_date, target_date)
    if wt:
        summaries = wt.get("dailyWeightSummaries", [])
        if summaries:
            latest_w = summaries[-1].get("allWeightMetrics", {})
            w_grams = latest_w.get("weight")
            result["weight_kg"] = round(w_grams / 1000, 1) if w_grams else None
            print(f"  Weight: {result.get('weight_kg')} kg")

    # ── Body Composition ──────────────────────
    print("📐 Body composition...")
    bc = safe_get(client.get_body_composition, "body_composition", target_date, target_date)
    if isinstance(bc, dict):
        avg = bc.get("totalAverage", {}) or {}
        result.update({
            "body_fat_percent":  avg.get("fatPercent"),
            "muscle_mass_grams": avg.get("muscleMass"),
            "bmi":               avg.get("bmi"),
        })
        if result.get("body_fat_percent"):
            print(f"  Body fat: {result['body_fat_percent']}%  BMI: {result['bmi']}")

    # ── Hydration ─────────────────────────────
    print("💧 Hydration...")
    hyd = safe_get(client.get_hydration_data, "hydration", target_date)
    if hyd:
        result.update({
            "hydration_ml":      hyd.get("valueInML"),
            "hydration_goal_ml": hyd.get("dailyIntakeGoalInML"),
        })
        print(f"  Intake: {result.get('hydration_ml')} ml  Goal: {result.get('hydration_goal_ml')} ml")

    # ── Activities ────────────────────────────
    print("🏋️  Activities...")
    acts = safe_get(client.get_activities_by_date, "activities", target_date, target_date)
    if acts:
        result["activities"] = []
        for act in acts:
            dur = int((act.get("duration") or 0) / 60)
            a = {
                "id":                       act.get("activityId"),
                "name":                     act.get("activityName"),
                "type":                     act.get("activityType", {}).get("typeKey"),
                "start_time":               act.get("startTimeLocal"),
                "duration_minutes":         dur,
                "distance_meters":          act.get("distance"),
                "calories":                 act.get("calories"),
                "avg_hr":                   act.get("averageHR"),
                "max_hr":                   act.get("maxHR"),
                "elevation_gain":           act.get("elevationGain"),
                "aerobic_training_effect":  act.get("aerobicTrainingEffect"),
                "anaerobic_training_effect":act.get("anaerobicTrainingEffect"),
            }
            result["activities"].append(a)
            print(f"  • {a['name']} ({a['type']}) {dur}min  HR {a['avg_hr']}-{a['max_hr']} bpm  {a['calories']} kcal")

    # ── Weekly Steps ──────────────────────────
    print("📅 Weekly steps...")
    today_d   = date.fromisoformat(target_date)
    week_start = str(today_d - timedelta(days=today_d.weekday()))
    try:
        ws = client.get_daily_steps(week_start, target_date)
        if isinstance(ws, list) and ws:
            result["weekly_steps_total"] = sum(w.get("totalSteps", 0) for w in ws)
            result["weekly_steps_avg"]   = result["weekly_steps_total"] // max(len(ws), 1)
            print(f"  This week: {result['weekly_steps_total']:,} total  {result['weekly_steps_avg']:,}/day avg")
    except Exception as e:
        print(f"  ⚠️  weekly_steps: {e}")

    # ── Save ──────────────────────────────────
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if target_date == str(date.today()):
        latest = os.path.join(DATA_DIR, "latest.json")
        with open(latest, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Saved: {cache_file}")
    return result


def show_latest():
    latest = os.path.join(DATA_DIR, "latest.json")
    if not os.path.exists(latest):
        print("No cached data. Run without --show first.")
        return
    with open(latest) as f:
        print(json.dumps(json.load(f), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch full Garmin Connect health data")
    parser.add_argument("--date",     help="Date YYYY-MM-DD (default: today)")
    parser.add_argument("--email",    help="Garmin account email")
    parser.add_argument("--password", help="Garmin account password (use env var GARMIN_PASSWORD instead to avoid shell history exposure)")
    parser.add_argument("--show",     action="store_true", help="Show latest cached data")
    parser.add_argument("--cn",       action="store_true", help="Use Garmin Connect CN (connect.garmin.com.cn) instead of global. Also settable via GARMIN_IS_CN=true env var. Recommended for Chinese Garmin accounts or mainland IP.")
    args = parser.parse_args()

    # Security: warn if password passed via CLI (visible in process list / shell history)
    if args.password:
        print("⚠️  Note: Passing --password via CLI may expose it in shell history.")
        print("   Prefer: export GARMIN_PASSWORD='yourpass'  or use macOS Keychain.\n")

    if args.show:
        show_latest()
    else:
        fetch_all(args.date, args.email, args.password, is_cn=args.cn or None)
