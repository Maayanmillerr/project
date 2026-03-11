#!/usr/bin/env python3
import argparse
from collections import Counter
from pathlib import Path
import re
import unicodedata

import pandas as pd


def normalize_step(step: str) -> str:
    s = unicodedata.normalize("NFC", (step or "").strip())
    s = s.replace("׳", "'").replace("״", '"')
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"_\d+$", "", s)

    # Normalize "עדכון ערך" variants:
    # "עדכון ערך - גיל" -> "גיל"
    # "עדכון ערך/..." is handled by path split, so only local cleanup here.
    s = re.sub(r"^עדכון ערך\s*-\s*", "", s)
    s = re.sub(r"^עדכון ערך\s+", "", s)

    if s == "עדכון ערך":
        return ""
    return s.strip()


def parse_path(route: str) -> list[str]:
    if route is None or (isinstance(route, float) and pd.isna(route)):
        return []
    raw_parts = str(route).split("/")
    steps = [normalize_step(p) for p in raw_parts]
    steps = [s for s in steps if s]

    # Collapse consecutive duplicates to avoid artificial loops from logging.
    deduped = []
    for s in steps:
        if not deduped or deduped[-1] != s:
            deduped.append(s)
    return deduped


def build_funnel(rows_steps: list[list[str]]) -> pd.DataFrame:
    entered = Counter()
    reached_next = Counter()
    drop_off = Counter()
    first_pos_sum = Counter()
    first_pos_cnt = Counter()

    for steps in rows_steps:
        if not steps:
            continue
        seen = set()
        for i, step in enumerate(steps):
            if step in seen:
                continue
            seen.add(step)
            entered[step] += 1
            first_pos_sum[step] += i + 1
            first_pos_cnt[step] += 1

            has_next_distinct = any(nxt != step for nxt in steps[i + 1 :])
            if has_next_distinct:
                reached_next[step] += 1
            else:
                drop_off[step] += 1

    data = []
    for step, entered_cnt in entered.items():
        reached = reached_next.get(step, 0)
        dropped = drop_off.get(step, entered_cnt - reached)
        data.append(
            {
                "step": step,
                "entered_step": entered_cnt,
                "reached_next_step": reached,
                "drop_off_at_step": dropped,
                "drop_off_rate": round(dropped / entered_cnt, 4) if entered_cnt else 0.0,
                "avg_first_position": round(first_pos_sum[step] / first_pos_cnt[step], 2),
            }
        )

    return pd.DataFrame(data).sort_values(
        by=["entered_step", "drop_off_at_step", "step"], ascending=[False, False, True]
    )


def main():
    parser = argparse.ArgumentParser(description="Parse route column and build funnel/drop-off metrics.")
    parser.add_argument(
        "-i",
        "--input",
        default="data/botcalls_merge_sheet1_codex.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--route-col",
        default="נתיב",
        help="Route column name",
    )
    parser.add_argument(
        "--parsed-output",
        default="data/botcalls_merge_sheet1_codex_with_route_steps.csv",
        help="Output CSV with parsed step columns",
    )
    parser.add_argument(
        "--funnel-output",
        default="data/botcalls_merge_sheet1_codex_funnel_steps.csv",
        help="Output CSV with funnel/drop-off summary",
    )
    args = parser.parse_args()

    inp = Path(args.input)
    out_parsed = Path(args.parsed_output)
    out_funnel = Path(args.funnel_output)
    if not inp.exists():
        print(f"ERROR: input file not found: {inp}")
        raise SystemExit(1)

    df = pd.read_csv(inp, dtype=str, encoding="utf-8-sig")
    if args.route_col not in df.columns:
        print(f"ERROR: route column '{args.route_col}' not found")
        raise SystemExit(2)

    route_steps = df[args.route_col].apply(parse_path)
    df["num_steps"] = route_steps.apply(len)
    df["normalized_path"] = route_steps.apply(lambda s: " / ".join(s))

    max_steps = int(df["num_steps"].max()) if len(df) else 0
    for i in range(1, max_steps + 1):
        df[f"step_{i}"] = route_steps.apply(lambda s, idx=i: s[idx - 1] if len(s) >= idx else "")

    out_parsed.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_parsed, index=False, encoding="utf-8-sig")

    funnel_df = build_funnel(route_steps.tolist())
    out_funnel.parent.mkdir(parents=True, exist_ok=True)
    funnel_df.to_csv(out_funnel, index=False, encoding="utf-8-sig")

    print(f"WROTE_PARSED: {out_parsed}")
    print(f"WROTE_FUNNEL: {out_funnel}")
    print(f"ROWS: {len(df)}")
    print(f"MAX_STEPS: {max_steps}")
    print(f"FUNNEL_STEPS: {len(funnel_df)}")


if __name__ == "__main__":
    main()
