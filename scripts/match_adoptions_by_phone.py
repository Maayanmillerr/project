#!/usr/bin/env python3
import argparse
from pathlib import Path
import re
import unicodedata

import pandas as pd


def clean_phone_digits(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    digits = re.sub(r"\D", "", str(value).strip())
    if digits.startswith("972"):
        digits = "0" + digits[3:]
    return digits


def phone_match_key(value) -> str:
    digits = clean_phone_digits(value)
    if not digits:
        return ""
    # Align to botcalls convention where most mobiles are stored without leading zero.
    if len(digits) == 10 and digits.startswith("0"):
        return digits[1:]
    return digits


def normalize_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = unicodedata.normalize("NFC", str(value)).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def detect_header_row(raw: pd.DataFrame) -> int:
    for i in range(min(50, len(raw))):
        row = [normalize_text(v) for v in raw.iloc[i].tolist()]
        row_set = set(row)
        if "טלפון" in row_set and "שם כלב" in row_set and "תאריך אימוץ" in row_set:
            return i
    raise ValueError("Could not detect header row with 'טלפון' + 'שם כלב' + 'תאריך אימוץ'")


def load_adoption_rows(xlsx_path: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)
    sheet = xl.sheet_names[0]
    raw = pd.read_excel(xlsx_path, sheet_name=sheet, header=None)
    hdr_idx = detect_header_row(raw)

    headers = [normalize_text(v) for v in raw.iloc[hdr_idx].tolist()]
    df = raw.iloc[hdr_idx + 1 :].copy()
    df.columns = headers
    df = df.reset_index(drop=True)

    for col in ["טלפון", "שם כלב", "תאריך אימוץ"]:
        if col not in df.columns:
            raise ValueError(f"Required column missing in adoption file: {col}")

    # Keep only candidate data rows (skip section title rows)
    df["טלפון"] = df["טלפון"].apply(clean_phone_digits)
    df["שם כלב"] = df["שם כלב"].apply(normalize_text)
    df["תאריך אימוץ"] = pd.to_datetime(df["תאריך אימוץ"], errors="coerce")
    df = df[(df["טלפון"] != "") & (df["שם כלב"] != "") & df["תאריך אימוץ"].notna()].copy()

    df["phone_key"] = df["טלפון"].apply(phone_match_key)
    df = df[df["phone_key"] != ""].copy()
    df["dog_name_norm"] = df["שם כלב"].str.lower()
    df["adoption_date"] = df["תאריך אימוץ"].dt.strftime("%Y-%m-%d")
    return df


def build_phone_adoption_summary(adf: pd.DataFrame) -> pd.DataFrame:
    # One adoption event per (phone, dog_name, adoption_date), per user rule.
    dedup = adf.drop_duplicates(subset=["phone_key", "dog_name_norm", "adoption_date"]).copy()

    summary = (
        dedup.groupby("phone_key", as_index=False)
        .agg(
            adoption_count=("adoption_date", "size"),
            adopted_dogs_nunique=("dog_name_norm", "nunique"),
            first_adoption_date=("adoption_date", "min"),
            last_adoption_date=("adoption_date", "max"),
        )
        .rename(columns={"phone_key": "phone"})
    )
    summary["success"] = (summary["adoption_count"] > 0).astype(int)
    return summary, dedup


def main():
    parser = argparse.ArgumentParser(description="Match adoption report to phone-level botcalls dataset by phone.")
    parser.add_argument(
        "--phone-level-input",
        default="data/botcalls_phone_level_codex.csv",
        help="Input phone-level codex CSV",
    )
    parser.add_argument(
        "--adoption-xlsx",
        required=True,
        help="Path to adoption Excel report",
    )
    parser.add_argument(
        "--output",
        default="data/botcalls_phone_level_codex.csv",
        help="Output labeled phone-level CSV (default: overwrite input)",
    )
    parser.add_argument(
        "--adoption-summary-output",
        default="data/adoptions_phone_summary_codex.csv",
        help="Output CSV with adoption summary per phone",
    )
    args = parser.parse_args()

    phone_path = Path(args.phone_level_input)
    adoption_path = Path(args.adoption_xlsx)
    out_path = Path(args.output)
    adoption_summary_path = Path(args.adoption_summary_output)

    if not phone_path.exists():
        print(f"ERROR: phone-level input not found: {phone_path}")
        raise SystemExit(1)
    if not adoption_path.exists():
        print(f"ERROR: adoption xlsx not found: {adoption_path}")
        raise SystemExit(2)

    phone_df = pd.read_csv(phone_path, dtype=str, encoding="utf-8-sig")
    if "phone" not in phone_df.columns:
        print("ERROR: phone-level input must include 'phone' column")
        raise SystemExit(3)

    phone_df["phone"] = phone_df["phone"].fillna("").astype(str).str.strip()
    phone_df["phone_key"] = phone_df["phone"].apply(phone_match_key)

    adoption_df = load_adoption_rows(adoption_path)
    summary_df, dedup_df = build_phone_adoption_summary(adoption_df)

    merged = phone_df.merge(
        summary_df,
        left_on="phone_key",
        right_on="phone",
        how="left",
        suffixes=("", "_adopt"),
    )

    for col, default in [
        ("adoption_count", 0),
        ("adopted_dogs_nunique", 0),
        ("success", 0),
    ]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(default).astype(int)
    for col in ["first_adoption_date", "last_adoption_date"]:
        merged[col] = merged[col].fillna("")

    merged = merged.drop(columns=["phone_adopt", "phone_key"], errors="ignore")
    merged = merged.sort_values(by=["success", "adoption_count", "sum_calls", "phone"], ascending=[False, False, False, True])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")

    adoption_summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.sort_values(by=["adoption_count", "phone"], ascending=[False, True]).to_csv(
        adoption_summary_path, index=False, encoding="utf-8-sig"
    )

    print(f"WROTE_LABELED: {out_path}")
    print(f"WROTE_ADOPTION_SUMMARY: {adoption_summary_path}")
    print(f"PHONE_ROWS: {len(merged)}")
    print(f"MATCHED_SUCCESS_PHONES: {int((merged['success'] == 1).sum())}")
    print(f"TOTAL_ADOPTIONS_MATCHED: {int(merged['adoption_count'].sum())}")
    print(f"RAW_ADOPTION_ROWS_USED: {len(adoption_df)}")
    print(f"DEDUP_ADOPTION_EVENTS: {len(dedup_df)}")


if __name__ == "__main__":
    main()
