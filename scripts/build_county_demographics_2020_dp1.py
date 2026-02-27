import json
import re
from pathlib import Path

import pandas as pd


RE_NON_KEY = re.compile(r"[^a-z0-9 .\\-]", flags=re.IGNORECASE)
RE_WS = re.compile(r"\\s+")


def norm_county_key(name: str) -> str:
    s = (name or "")
    s = RE_NON_KEY.sub("", s)
    s = RE_WS.sub(" ", s).strip().upper()
    return s


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    dp1 = repo_root / "data" / "tmp" / "dp1_extract" / "DECENNIALDP2020.DP1-Data.csv"
    out = repo_root / "data" / "county_demographics_2020_dp1.json"

    if not dp1.exists():
        raise SystemExit(f"Missing input: {dp1}")

    usecols = [
        "NAME",
        "DP1_0001C",  # total population
        "DP1_0021C",  # total 18 years and over (VAP total)
        "DP1_0078C",  # White (one race)
        "DP1_0079C",  # Black (one race)
        "DP1_0093C",  # Hispanic or Latino (of any race)
    ]
    df = pd.read_csv(dp1, usecols=usecols, dtype=str)

    rows = {}
    for _, r in df.iterrows():
        name = (r.get("NAME") or "").strip()
        if not name.endswith("County, North Carolina"):
            continue

        county = name.replace("County, North Carolina", "").strip()
        key = norm_county_key(county)
        if not key:
            continue

        def as_int(v):
            try:
                return int(str(v).replace(",", "").strip())
            except Exception:
                return 0

        total_pop = as_int(r.get("DP1_0001C"))
        vap_18 = as_int(r.get("DP1_0021C"))
        white = as_int(r.get("DP1_0078C"))
        black = as_int(r.get("DP1_0079C"))
        hisp = as_int(r.get("DP1_0093C"))

        def pct(x):
            return round((x / total_pop * 100.0), 2) if total_pop else 0.0

        rows[key] = {
            "county": county,
            "total_pop": total_pop,
            "vap_18plus": vap_18,
            # These race/ethnicity values are for total population in DP1.
            "white_pop": white,
            "black_pop": black,
            "hispanic_pop": hisp,
            "white_pop_pct": pct(white),
            "black_pop_pct": pct(black),
            "hispanic_pop_pct": pct(hisp),
        }

    payload = {
        "source": "DECENNIALDP2020.DP1",
        "notes": [
            "vap_18plus is total population 18 years and over (VAP total).",
            "Race/ethnicity percentages are for total population (DP1), not VAP-by-race.",
        ],
        "counties": rows,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
        f.write("\n")

    print(f"Wrote {out} ({len(rows)} counties)")


if __name__ == "__main__":
    main()

