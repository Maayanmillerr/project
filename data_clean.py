import os, re, zipfile
from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(".")  # שימי כאן את התיקייה עם הקבצים
ZIP_PATH = BASE / "OneDrive_1_3-2-2026.zip"
ADOPTIONS_PATH = BASE / "דוח אימוצים חשבשבת 2019_2025.xlsx"
EXTRA_CALLS_PATH = BASE / "03_01_2026__21_53__דוח_קריאות_מפורט_שירות_למועמדים_1.xlsx"

OUT_DIR = BASE / "clean_outputs"
OUT_DIR.mkdir(exist_ok=True)

def normalize_il_phone(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    digits = re.sub(r"\D", "", s)
    if digits.startswith("00972"):
        digits = "0" + digits[5:]
    elif digits.startswith("972"):
        digits = "0" + digits[3:]
    if len(digits) < 9 or len(digits) > 10:
        return np.nan
    return digits

def td_to_minutes(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, pd.Timedelta):
        return x.total_seconds() / 60
    try:
        return pd.to_timedelta(x).total_seconds() / 60
    except:
        return np.nan

def read_first_sheet(path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    return pd.read_excel(path, sheet_name=xl.sheet_names[0])

# 1) Extract ZIP (call reports)
extract_dir = OUT_DIR / "_zip_extract"
extract_dir.mkdir(exist_ok=True)
with zipfile.ZipFile(ZIP_PATH, "r") as z:
    z.extractall(extract_dir)

call_paths = [EXTRA_CALLS_PATH] if EXTRA_CALLS_PATH.exists() else []
call_paths = call_paths + list(extract_dir.rglob("*.xlsx"))
call_paths = call_paths + list(extract_dir.rglob("*.xlsx"))

# 2) Load + union
dfs = []
for p in call_paths:
    df = read_first_sheet(p)
    df["source_file"] = p.name
    dfs.append(df)
calls = pd.concat(dfs, ignore_index=True, sort=False)

# 3) Normalize phone (candidate phone = מזהה לקוח)
calls["phone"] = calls["מזהה לקוח"].apply(normalize_il_phone)
calls["open_time"] = pd.to_datetime(calls["פתיחת קריאה"], errors="coerce")
calls["update_time"] = pd.to_datetime(calls["עדכון אחרון"], errors="coerce")

# 4) Deduplicate by call id
calls = calls.sort_values(["מספר קריאה", "update_time"]).drop_duplicates(["מספר קריאה"], keep="last")

# 5) Load adoptions (header row=2) + normalize phone
xl = pd.ExcelFile(ADOPTIONS_PATH)
adopt = pd.read_excel(ADOPTIONS_PATH, sheet_name=xl.sheet_names[0], header=2)
adopt = adopt[adopt["תאריך אימוץ"].notna()].copy()
adopt["phone"] = adopt["טלפון"].apply(normalize_il_phone)
adopt = adopt[adopt["phone"].notna()].copy()

adopt_by_phone = adopt.groupby("phone").agg(
    first_adoption_date=("תאריך אימוץ", "min"),
    last_adoption_date=("תאריך אימוץ", "max"),
    num_adoptions=("תאריך אימוץ", "nunique"),
).reset_index()

# 6) Cutoff to prevent leakage: keep calls until adoption date (if exists), else end of data
end_date = calls["open_time"].max().normalize()
calls = calls.merge(adopt_by_phone, on="phone", how="left")
calls["has_adoption"] = calls["first_adoption_date"].notna()
calls["cutoff_date"] = np.where(calls["has_adoption"], calls["first_adoption_date"], end_date)
calls["cutoff_date"] = pd.to_datetime(calls["cutoff_date"], errors="coerce")
calls["open_date"] = calls["open_time"].dt.normalize()
calls_feat = calls[calls["open_date"] <= calls["cutoff_date"]].copy()

# 7) Feature engineering (example)
for col in ["הודעות נכנסות", "הודעות יוצאות"]:
    calls_feat[col] = pd.to_numeric(calls_feat[col], errors="coerce").fillna(0)

# Convert age to numeric
calls_feat["גיל המועמד"] = pd.to_numeric(calls_feat["גיל המועמד"], errors="coerce")

for col in ["זמן שיחה נטו", "זמן טיפול נטו"]:
    if col in calls_feat.columns:
        calls_feat[col + "_min"] = calls_feat[col].apply(td_to_minutes)

# 8) Candidate aggregation
def mode_series(s):
    s = s.dropna()
    return s.value_counts().idxmax() if len(s) else np.nan


def extract_real_city(s):
    # Filter out non-city responses (יכול/ה, התחלה, etc.)
    s = s.dropna()
    s = s[~s.str.contains("יכול|התחלה|דירה|מסעדה|מושב|45|ניו", na=False, regex=True)]
    return s.value_counts().idxmax() if len(s) else np.nan

candidates = calls_feat.groupby("phone").agg(
    first_contact=("open_time", "min"),
    last_contact=("open_time", "max"),
    num_conversations=("מספר קריאה", "count"),
    total_in_msgs=("הודעות נכנסות", "sum"),
    total_out_msgs=("הודעות יוצאות", "sum"),
    total_chat_min=("זמן שיחה נטו_min", "sum"),
    candidate_age=("גיל המועמד", "mean"),
    city=("יישוב מגורים", extract_real_city),
    apartment_type=("סוג דירה", mode_series),
).reset_index()

candidates["days_active"] = (candidates["last_contact"] - candidates["first_contact"]).dt.total_seconds() / 86400
candidates["y_adopted"] = candidates["phone"].isin(set(adopt_by_phone["phone"])).astype(int)
candidates = candidates.merge(adopt_by_phone, on="phone", how="left")

# 9) Save outputs
calls.to_excel(OUT_DIR / "calls_clean_dedup.xlsx", index=False)
candidates.to_excel(OUT_DIR / "candidates_features_with_target.xlsx", index=False)