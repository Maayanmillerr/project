#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd
import re
from difflib import get_close_matches
import unicodedata
from collections import OrderedDict

def clean_phone_value(v):
    if pd.isna(v):
        return v
    s = str(v).strip()
    digits = re.sub(r"\D", "", s)
    if digits.startswith("972"):
        return "0" + digits[3:]
    return digits


def contains_hebrew(text: str) -> bool:
    return bool(re.search(r"[\u0590-\u05FF]", text or ""))


def norm_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFC", s).strip()
    s = s.replace("׳", "'").replace("״", '"')
    s = re.sub(r"[(){}\[\]]", " ", s)
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def norm_city_key(s: str) -> str:
    s = norm_text(s).lower()
    s = re.sub(r"^ק\.?\s+", "קרית ", s)
    s = re.sub(r"^ק'\s+", "קרית ", s)
    s = re.sub(r"\bקריית\b", "קרית", s)
    s = re.sub(r"[\"'`.,:;!?]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def preferred_name(name: str) -> str:
    return norm_text(name).replace("קריית", "קרית")


def load_city_aliases(inp: Path):
    aliases = OrderedDict()

    def add_alias(alias: str, canonical: str):
        a = norm_city_key(alias)
        c = preferred_name(canonical)
        if a and c and a not in aliases:
            aliases[a] = c

    # Primary source: cities_israel.py helper (loads israel_data.py when available)
    base_cities = []
    try:
        from cities_israel import load_cities

        base_cities = load_cities()
    except Exception:
        try:
            from scripts.cities_israel import load_cities

            base_cities = load_cities()
        except Exception:
            base_cities = []

    # Fallback: local text file
    file_cities = []
    cities_file = inp.parent / "israel_cities.txt"
    if cities_file.exists():
        with open(cities_file, "r", encoding="utf-8") as fh:
            file_cities = [line.strip() for line in fh if line.strip()]

    for c in base_cities:
        add_alias(c, c)

    # Pair English+Hebrew lines when possible and map both to the Hebrew canonical value.
    i = 0
    while i < len(file_cities):
        cur = file_cities[i]
        nxt = file_cities[i + 1] if i + 1 < len(file_cities) else ""
        if not contains_hebrew(cur) and contains_hebrew(nxt):
            add_alias(cur, nxt)
            add_alias(nxt, nxt)
            i += 2
            continue
        add_alias(cur, cur)
        i += 1

    # Manual high-frequency aliases.
    manual_aliases = {
        "י'ם": "ירושלים",
        'י"ם': "ירושלים",
        "תא": "תל אביב יפו",
        "תל אביב": "תל אביב יפו",
        "תל אביב יפו": "תל אביב יפו",
        "tel aviv": "תל אביב יפו",
        "tel aviv yafo": "תל אביב יפו",
        "jerusalem": "ירושלים",
        "beer sheva": "באר שבע",
        "beersheba": "באר שבע",
    }
    for a, c in manual_aliases.items():
        add_alias(a, c)

    canonical_norm = set(norm_city_key(v) for v in aliases.values())
    return aliases, canonical_norm


def best_city_match(raw_value: str, aliases: dict, canonical_norm: set):
    text = norm_text(raw_value)
    if not text:
        return "", ""

    # Keep only the first logical value if multiple values were entered.
    first = re.split(r"[,/;\\\\|]", text)[0].strip()
    first = re.sub(r"\bאזור\b.*$", "", first).strip()
    first = re.sub(r"\bגר(?:ה|ים|ות)?\b.*$", "", first).strip()
    if not first:
        return "", ""

    raw_key = norm_city_key(first)
    if not raw_key:
        return "", ""
    if raw_key in aliases:
        return aliases[raw_key], ""

    # Try contiguous word windows, so phrases like "יד אליהו תל אביב" can still map.
    words = raw_key.split()
    windows = []
    for size in range(min(4, len(words)), 0, -1):
        for start in range(0, len(words) - size + 1):
            cand = " ".join(words[start : start + size])
            if cand not in windows:
                windows.append(cand)

    for w in windows:
        if w in aliases:
            return aliases[w], ""

    # High-confidence typo correction.
    search_space = list(aliases.keys())
    for w in windows:
        if not contains_hebrew(w) and len(w) < 4:
            continue
        if len(w) <= 4:
            cutoff = 0.95
        elif len(w) <= 7:
            cutoff = 0.92
        else:
            cutoff = 0.88
        matches = get_close_matches(w, search_space, n=1, cutoff=cutoff)
        if matches:
            return aliases[matches[0]], ""

    # Provide one suggestion for the unmatched report.
    suggestion = ""
    fallback = get_close_matches(raw_key, search_space, n=1, cutoff=0.82)
    if fallback:
        suggestion = aliases[fallback[0]]

    # Unmatched values are blanked to keep only valid Israeli localities.
    return "", suggestion

def main():
    parser = argparse.ArgumentParser(description="Clean botcalls CSV (phone column)")
    parser.add_argument("-i", "--input", help="input CSV path", default="data/botcalls_merge_sheet1.csv")
    parser.add_argument("-o", "--output", help="output CSV path (defaults to input, overwrites in-place if same)")
    parser.add_argument(
        "--route-contains",
        default="סיבת הפניה אימוץ כלב",
        help="keep only rows where column 'נתיב' contains this text; pass empty string to disable",
    )
    args = parser.parse_args()

    # inputs/outputs (out may be same as inp)
    inp = Path(args.input)
    out = Path(args.output) if args.output else inp
    if not inp.exists():
        print(f"ERROR: input file not found: {inp}")
        raise SystemExit(1)

    df = pd.read_csv(str(inp), dtype=str, encoding="utf-8-sig")

    # ROUTE FILTER (נתיב): keep only rows in the adoption intent path.
    if args.route_contains:
        route_col = "נתיב"
        if route_col not in df.columns:
            print(f"ERROR: required column '{route_col}' not found for route filter.")
            raise SystemExit(3)
        before = len(df)
        mask = df[route_col].fillna("").astype(str).str.contains(args.route_contains, regex=False)
        df = df.loc[mask].copy()
        print(f"ROUTE_FILTER: kept {len(df)} / {before} rows where '{route_col}' contains '{args.route_contains}'")

    # PHONE: accept either 'מזהה לקוח' or already-renamed 'phone'
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(clean_phone_value)
    elif "מזהה לקוח" in df.columns:
        df = df.rename(columns={"מזהה לקוח": "phone"})
        df["phone"] = df["phone"].apply(clean_phone_value)
    else:
        print("ERROR: column 'מזהה לקוח' or 'phone' not found. Available columns:", df.columns.tolist())
        raise SystemExit(2)

    # CITY (יישוב מגורים)
    city_col = "יישוב מגורים"
    if "city" in df.columns or city_col in df.columns:
        if city_col in df.columns and "city" not in df.columns:
            df = df.rename(columns={city_col: "city"})
        aliases, canonical_norm = load_city_aliases(inp)
        unmatched = OrderedDict()

        def clean_city_cell(v):
            if pd.isna(v):
                return ""
            raw = str(v).strip()
            if not raw:
                return ""
            cleaned, suggestion = best_city_match(raw, aliases, canonical_norm)
            if not cleaned:
                unmatched.setdefault(raw, suggestion)
            return cleaned

        df["city"] = df["city"].apply(clean_city_cell)
        # Enforce validity: keep only canonical city values or empty.
        invalid_mask = df["city"].notna() & (df["city"] != "") & ~df["city"].map(
            lambda x: norm_city_key(str(x)) in canonical_norm
        )
        if invalid_mask.any():
            df.loc[invalid_mask, "city"] = ""

        # write unmatched report with one suggested correction (if found)
        report_path = out.parent / "btcallls_city_unmatched.txt"
        with open(report_path, "w", encoding="utf-8") as rf:
            for raw, sugg in sorted(unmatched.items()):
                if sugg:
                    rf.write(f"{raw}\t->\t{sugg}\n")
                else:
                    rf.write(raw + "\n")
        print("WROTE_CITY_UNMATCHED:", report_path)

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(str(out), index=False, encoding="utf-8-sig")
    print("WROTE:", out)

if __name__ == "__main__":
    main()
