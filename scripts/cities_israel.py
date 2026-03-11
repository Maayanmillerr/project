import urllib.request
import json
import ssl
import csv
import io
import os
from pathlib import Path
import sys
import unicodedata
import re
from difflib import get_close_matches


def _contains_hebrew(s: str) -> bool:
    return bool(re.search(r'[\u0590-\u05FF]', s))

def create_local_library():
    """Build a local city list and write israel_data.py.

    Primary source: CBS Excel of municipalities via pandas.
    Secondary source: government CSV as before.
    Tertiary: local text fallback.
    """
    # try pandas source first
    try:
        import pandas as pd
    except ImportError:
        pd = None

    cities = []

    # 1. CBS excel file
    if pd is not None:
        try:
            print("Downloading cities list from CBS spreadsheet...")
            url = "https://www.cbs.gov.il/he/publications/doclib/2019/ishuvim/bycode2021.xlsx"
            df = pd.read_excel(url)
            if "שם יישוב" in df.columns:
                cities = df["שם יישוב"].dropna().astype(str).tolist()
                cities = sorted(list(set(cities)))
                print(f"Loaded {len(cities)} names from CBS Excel.")
        except Exception as e:
            print(f"CBS spreadsheet fetch failed: {e}")
            cities = []

    # 2. government CSV fallback
    if not cities:
        # Bypass SSL verification for macOS
        context = ssl._create_unverified_context()
        csv_url = "https://data.gov.il/dataset/3bd78393-277d-4171-8575-b6d4e17f7d46/resource/351271b1-3f33-4f05-bc50-f896944e0573/download/yishuvim.csv"
        try:
            print(f"Connecting to government storage...")
            request = urllib.request.Request(csv_url)
            with urllib.request.urlopen(request, context=context) as response:
                content = response.read().decode('cp1255')
            if content.lstrip().startswith('<'):
                print("Warning: Remote resource returned HTML instead of CSV. Falling back to local list.")
            else:
                csv_reader = csv.DictReader(io.StringIO(content))
                for row in csv_reader:
                    city_name = row.get('שם_יישוב') or row.get('שם הישוב') or row.get('name')
                    if city_name:
                        cities.append(city_name.strip())
                cities = sorted(list(set(cities)))
        except Exception as e:
            print(f"Government CSV fetch failed: {e}")
            cities = []

    # 3. local file fallback
    if not cities:
        local_file = Path(__file__).resolve().parents[1] / 'data' / 'israel_cities.txt'
        if local_file.exists():
            print(f"Loading local city list from {local_file}")
            with open(local_file, 'r', encoding='utf-8') as lf:
                lines = [ln.strip() for ln in lf if ln.strip()]
            hebrew_names = [ln for ln in lines if _contains_hebrew(ln)]
            cities = sorted(list(set(hebrew_names or lines)))
        else:
            print("Error: The list is empty and no local fallback found.")
            return

    # final set of cities
    cities = sorted(list(set(cities)))
    with open('israel_data.py', 'w', encoding='utf-8') as f:
        f.write(f"CITIES = {json.dumps(cities, ensure_ascii=False, indent=4)}")
    print("------------------------------------------")
    print(f"SUCCESS: File 'israel_data.py' created!")
    print(f"TOTAL LOCATIONS: {len(cities)}")
    print("------------------------------------------")


def _load_cities_from_generated():
    """Try importing the generated israel_data.py (created by this script)."""
    try:
        # add project root to path so import works when running from scripts/
        proj_root = Path(__file__).resolve().parents[1]
        if str(proj_root) not in sys.path:
            sys.path.insert(0, str(proj_root))
        import israel_data
        return list(israel_data.CITIES)
    except Exception:
        return None


def _load_cities_fallback():
    local_file = Path(__file__).resolve().parents[1] / 'data' / 'israel_cities.txt'
    cities = []
    if local_file.exists():
        with open(local_file, 'r', encoding='utf-8') as lf:
            lines = [ln.strip() for ln in lf if ln.strip()]
        # Prefer Hebrew names if present in the file (many files contain English/Hebrew pairs)
        hebrew_names = [ln for ln in lines if _contains_hebrew(ln)]
        if hebrew_names:
            cities = sorted(list(set(hebrew_names)))
        else:
            cities = sorted(list(set(lines)))
    return cities


def load_cities():
    """Return list of cities, prefer generated file, fall back to data/israel_cities.txt."""
    cities = _load_cities_from_generated()
    if cities:
        return cities
    return _load_cities_fallback()


def normalize_name(s: str) -> str:
    """Normalize city name for comparison: strip, lowercase, remove punctuation and diacritics."""
    if not s:
        return ''
    s = s.strip()
    # Unicode normalize (decompose) and remove combining marks (diacritics)
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    # Remove punctuation (keep letters, numbers, and Hebrew block)
    s = re.sub(r"[^\w\s\u0590-\u05FF-]", '', s)
    # Normalize hyphens and whitespace
    s = re.sub(r"[-_]+", ' ', s)
    s = re.sub(r"\s+", ' ', s)
    return s.lower()




def _build_lookup(cities):
    """Create mapping normalized -> hebrew name, including transliterations.

    A small manual dictionary complements the imperfect unidecode output.
    """
    lookup = {}
    try:
        from unidecode import unidecode
    except ImportError:
        unidecode = lambda x: x

    for c in cities:
        n = normalize_name(c)
        lookup[n] = c
        # also map transliterated/ASCII form and prefixes
        en = unidecode(c)
        if en and en != c:
            ne = normalize_name(en)
            lookup[ne] = c
            # also add prefixes of words (e.g. "tel aviv" from "tel aviv yfo")
            parts = ne.split()
            if len(parts) > 1:
                for i in range(1, len(parts)):
                    prefix = ' '.join(parts[:i])
                    lookup[prefix] = c

    # manual English-to-Hebrew fallbacks for common names
    manual_map = {
        'jerusalem': 'ירושלים',
        'tel aviv': 'תל אביב-יפו',
        'tel aviv yafo': 'תל אביב-יפו',
        'beer sheva': 'באר שבע',
        'beersheba': 'באר שבע',
        'kfar yona': 'כפר יונה',
        'kiryat yam': 'קריית ים',
        'kfar shalem': 'כפר שלם',
        'gan ner': 'גן נר',
        # add more if needed
    }
    for en, he in manual_map.items():
        lookup[normalize_name(en)] = he
    return lookup



def city_exists(query: str, cities=None) -> bool:
    """Return True if query matches a city in the list (normalized exact match).

    English transliterations are accepted and mapped to Hebrew.
    """
    if cities is None:
        cities = load_cities()
    lookup = _build_lookup(cities)
    norm_query = normalize_name(query)
    return norm_query in lookup


def suggest_cities(query: str, cities=None, n=5):
    """Return up to `n` close matches for `query` from `cities` using difflib.

    English transliterations also considered via lookup.
    """
    if cities is None:
        cities = load_cities()
    lookup = _build_lookup(cities)
    matches = get_close_matches(normalize_name(query), list(lookup.keys()), n=n, cutoff=0.6)
    return [lookup[m] for m in matches]


def validate_csv_cities(csv_path: str, column: str = 'city') -> dict:
    """Read `csv_path`, check values in `column` against known cities.

    Returns dict with keys:
      - "valid": set of normalized matching names
      - "invalid": dict value -> suggestion list
      - "empty_count": number of blank entries
      - "total": total rows processed
    """

    results = {'valid': set(), 'invalid': {}, 'empty_count': 0, 'total': 0}
    cities = load_cities()
    lookup = _build_lookup(cities)  # Build lookup once, not per row
    
    with open(csv_path, newline='', encoding='utf-8') as cf:
        reader = csv.DictReader(cf)
        if column not in reader.fieldnames:
            raise ValueError(f"Column '{column}' not found in {csv_path}")
        for row in reader:
            results['total'] += 1
            val = row.get(column, '').strip()
            if not val:
                results['empty_count'] += 1
                continue
            norm_query = normalize_name(val)
            if norm_query in lookup:
                results['valid'].add(val)
            else:
                sugg = get_close_matches(norm_query, list(lookup.keys()), n=3, cutoff=0.6)
                results['invalid'][val] = [lookup[m] for m in sugg]
    return results

if __name__ == "__main__":
    # CLI: keep previous behaviour when no args provided
    if len(sys.argv) >= 2:
        cmd = sys.argv[1]
        if cmd == 'check':
            if len(sys.argv) < 3:
                print('Usage: python3 scripts/cities_israel.py check "city name"')
                sys.exit(2)
            q = sys.argv[2]
            cities = load_cities()
            exists = city_exists(q, cities=cities)
            if exists:
                print(f"FOUND: '{q}' is in the city list.")
                sys.exit(0)
            else:
                print(f"NOT FOUND: '{q}' is not in the city list.")
                sugg = suggest_cities(q, cities=cities, n=5)
                if sugg:
                    print('Did you mean:')
                    for s in sugg:
                        print(' -', s)
                sys.exit(1)
        elif cmd == 'validate':
            if len(sys.argv) < 3:
                print('Usage: python3 scripts/cities_israel.py validate path/to/file.csv [column]')
                sys.exit(2)
            path = sys.argv[2]
            col = sys.argv[3] if len(sys.argv) >= 4 else 'city'
            try:
                res = validate_csv_cities(path, column=col)
            except Exception as e:
                print(f"Validation failed: {e}")
                sys.exit(3)
            print(f"Processed {res['total']} rows, {res['empty_count']} empty '{col}' values.")
            if res['invalid']:
                print(f"Found {len(res['invalid'])} invalid entries:")
                for val, sugg in res['invalid'].items():
                    print(f"  '{val}' -> suggestions: {sugg}")
                sys.exit(1)
            else:
                print('All non-empty values are valid city names.')
                sys.exit(0)
        else:
            # default behaviour: regenerate list
            create_local_library()
    else:
        create_local_library()

# Note: main entry handled above to allow CLI; no duplicate run here.