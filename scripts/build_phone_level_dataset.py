#!/usr/bin/env python3
import argparse
from pathlib import Path
import re

import pandas as pd


def non_empty(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s[s != ""]


def mode_non_empty(series: pd.Series) -> str:
    s = non_empty(series)
    if s.empty:
        return ""
    return s.value_counts().idxmax()


def last_non_empty(series: pd.Series) -> str:
    s = non_empty(series)
    if s.empty:
        return ""
    return s.iloc[-1]


def parse_duration_to_seconds(value) -> float:
    if value is None:
        return float("nan")
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return float("nan")

    s = s.replace(",", ".")
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
            if len(parts) == 3:
                hours = float(parts[0])
                minutes = float(parts[1])
                seconds = float(parts[2])
                return hours * 3600 + minutes * 60 + seconds
        except ValueError:
            return float("nan")

    # plain numeric fallback (already seconds)
    try:
        return float(s)
    except ValueError:
        return float("nan")


def main():
    parser = argparse.ArgumentParser(description="Build phone-level aggregated dataset from codex call-level file.")
    parser.add_argument(
        "-i",
        "--input",
        default="data/botcalls_merge_sheet1_codex.csv",
        help="Input call-level CSV",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="data/botcalls_phone_level_codex.csv",
        help="Output phone-level CSV",
    )
    args = parser.parse_args()

    inp = Path(args.input)
    out = Path(args.output)
    if not inp.exists():
        print(f"ERROR: input file not found: {inp}")
        raise SystemExit(1)

    df = pd.read_csv(inp, dtype=str, encoding="utf-8-sig")
    if "phone" not in df.columns:
        print("ERROR: required column 'phone' not found.")
        raise SystemExit(2)

    df["phone"] = df["phone"].fillna("").astype(str).str.strip()
    df = df[df["phone"] != ""].copy()
    df["_row_idx"] = range(len(df))

    for col in ["הודעות נכנסות", "הודעות יוצאות", "מדיה נכנסת", "מדיה יוצאת", "num_steps"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    duration_cols = {
        "זמן המתנה בתור": "queue_wait",
        "זמן תגובה ללקוח": "customer_response",
        "זמן מענה נציג ראשוני": "first_agent_response",
        "זמן טיפול": "handling",
        "זמן שיחה נטו": "chat_net",
        "ממוצע תגובה להודעות נטו": "avg_msg_response_net",
        'סה"כ זמן שיחה פעילה': "active_call_total",
        'סה"כ זמן נטו של שיחה פעילה': "active_call_total_net",
    }
    for src, prefix in duration_cols.items():
        if src in df.columns:
            df[f"{prefix}_sec"] = df[src].apply(parse_duration_to_seconds)

    rows = []
    for phone, g in df.groupby("phone", sort=False):
        row = {
            "phone": phone,
            "sum_calls": int(len(g)),
            "call_ids_nunique": int(non_empty(g["מספר קריאה"]).nunique()) if "מספר קריאה" in g else 0,
            "first_row_idx": int(g["_row_idx"].min()),
            "last_row_idx": int(g["_row_idx"].max()),
            "status_mode": mode_non_empty(g["סטטוס"]) if "סטטוס" in g else "",
            "status_latest": last_non_empty(g["סטטוס"]) if "סטטוס" in g else "",
            "status_nunique": int(non_empty(g["סטטוס"]).nunique()) if "סטטוס" in g else 0,
            "city_mode": mode_non_empty(g["city"]) if "city" in g else "",
            "city_latest": last_non_empty(g["city"]) if "city" in g else "",
            "city_nunique": int(non_empty(g["city"]).nunique()) if "city" in g else 0,
            "step1_mode": mode_non_empty(g["step_1"]) if "step_1" in g else "",
            "paths_nunique": int(non_empty(g["normalized_path"]).nunique()) if "normalized_path" in g else 0,
            "top_path": mode_non_empty(g["normalized_path"]) if "normalized_path" in g else "",
            "num_steps_mean": round(pd.to_numeric(g["num_steps"], errors="coerce").mean(), 3)
            if "num_steps" in g
            else float("nan"),
            "num_steps_max": int(pd.to_numeric(g["num_steps"], errors="coerce").max())
            if "num_steps" in g and pd.to_numeric(g["num_steps"], errors="coerce").notna().any()
            else 0,
            "incoming_messages_sum": int(pd.to_numeric(g["הודעות נכנסות"], errors="coerce").fillna(0).sum())
            if "הודעות נכנסות" in g
            else 0,
            "outgoing_messages_sum": int(pd.to_numeric(g["הודעות יוצאות"], errors="coerce").fillna(0).sum())
            if "הודעות יוצאות" in g
            else 0,
            "incoming_media_sum": int(pd.to_numeric(g["מדיה נכנסת"], errors="coerce").fillna(0).sum())
            if "מדיה נכנסת" in g
            else 0,
            "outgoing_media_sum": int(pd.to_numeric(g["מדיה יוצאת"], errors="coerce").fillna(0).sum())
            if "מדיה יוצאת" in g
            else 0,
        }

        for _, prefix in duration_cols.items():
            col = f"{prefix}_sec"
            if col in g:
                s = pd.to_numeric(g[col], errors="coerce")
                row[f"{prefix}_sec_mean"] = round(s.mean(), 3) if s.notna().any() else float("nan")
                row[f"{prefix}_sec_max"] = round(s.max(), 3) if s.notna().any() else float("nan")
                row[f"{prefix}_sec_sum"] = round(s.fillna(0).sum(), 3)

        rows.append(row)

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(by=["sum_calls", "phone"], ascending=[False, True]).reset_index(drop=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"WROTE: {out}")
    print(f"PHONE_ROWS: {len(out_df)}")
    print(f"CALL_ROWS_USED: {len(df)}")
    print(f"AVG_CALLS_PER_PHONE: {round(len(df) / len(out_df), 3) if len(out_df) else 0}")


if __name__ == "__main__":
    main()
