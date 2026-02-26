"""
Build district-level election results on NC court-ordered 2022 lines by
reallocating precinct results using precomputed area-weighted crosswalks.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
import geopandas as gpd


TARGET_CRS = "EPSG:5070"
PLAN_ID = "nc_court_ordered_2022"
PLAN_LABEL = "NC Court-Ordered 2022 Lines (used for 2022 cycle)"
COMMON_PRECINCT_WORDS = [
    "PRECINCT",
    "PCT",
    "PRCT",
    "VTD",
    "WARD",
]


def calculate_competitiveness(margin_pct: float) -> str:
    abs_margin = abs(margin_pct)
    if abs_margin < 0.5:
        return "#f7f7f7"
    rep_win = margin_pct > 0
    if abs_margin >= 40:
        return "#67000d" if rep_win else "#08306b"
    if abs_margin >= 30:
        return "#a50f15" if rep_win else "#08519c"
    if abs_margin >= 20:
        return "#cb181d" if rep_win else "#3182bd"
    if abs_margin >= 10:
        return "#ef3b2c" if rep_win else "#6baed6"
    if abs_margin >= 5.5:
        return "#fb6a4a" if rep_win else "#9ecae1"
    if abs_margin >= 1:
        return "#fcae91" if rep_win else "#c6dbef"
    return "#fee8c8" if rep_win else "#e1f5fe"


def load_crosswalk(path: Path, key_col: str = "precinct_key") -> dict[str, list[tuple[str, float]]]:
    df = pd.read_csv(path, dtype={"district": str})
    if key_col not in df.columns:
        raise ValueError(f"{path} missing key column: {key_col}")
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for _, r in df.iterrows():
        key = str(r[key_col]).strip().upper()
        out[key].append((str(r["district"]).strip(), float(r["area_weight"])))
    return out


def build_county_fallback_map(
    path: Path,
    dominant_threshold: float | None = 0.995,
) -> dict[str, list[tuple[str, float]]]:
    """
    Build county-level fallback weights from precinct crosswalks.
    If dominant_threshold is set, and a county is effectively a whole-county
    cluster in one district (top share >= dominant_threshold), collapse to
    100% top district and skip split counties.
    If dominant_threshold is None, return full county shares for all counties.
    """
    df = pd.read_csv(path, dtype={"district": str})
    county_rows: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for _, r in df.iterrows():
        pk = str(r["precinct_key"]).strip().upper()
        if " - " not in pk:
            continue
        county, _ = pk.split(" - ", 1)
        district = str(r["district"]).strip()
        county_rows[county][district] += float(r["area_weight"])

    out: dict[str, list[tuple[str, float]]] = {}
    for county, dist_map in county_rows.items():
        total = sum(dist_map.values())
        if total <= 0:
            continue
        shares = sorted(
            ((d, w / total) for d, w in dist_map.items()),
            key=lambda x: x[1],
            reverse=True,
        )
        if dominant_threshold is None:
            out[county] = shares
            continue
        if shares and shares[0][1] >= dominant_threshold:
            out[county] = [(shares[0][0], 1.0)]
    return out


def build_precinct_to_vtd_map(
    *,
    voting_geojson: Path,
    vtd_path: Path,
) -> dict[str, str]:
    """
    Build a robust precinct_key -> vtd_geoid20 bridge by max overlap area.
    """
    p = gpd.read_file(voting_geojson)[["county_nam", "prec_id", "geometry"]].copy()
    p["precinct_key"] = (
        p["county_nam"].astype(str).str.strip().str.upper()
        + " - "
        + p["prec_id"].astype(str).str.strip().str.upper()
    )
    p = p.to_crs(TARGET_CRS)

    v = gpd.read_file(vtd_path)
    geoid_col = "GEOID20" if "GEOID20" in v.columns else "GEOID"
    v = v[[geoid_col, "geometry"]].copy().rename(columns={geoid_col: "vtd_geoid20"})
    v = v.to_crs(TARGET_CRS)

    inter = gpd.overlay(
        p[["precinct_key", "geometry"]],
        v[["vtd_geoid20", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    inter["a"] = inter.geometry.area
    inter = inter[inter["a"] > 0].copy()
    if inter.empty:
        return {}

    inter = inter.sort_values(["precinct_key", "a"], ascending=[True, False])
    top = inter.drop_duplicates(subset=["precinct_key"], keep="first")
    return {
        str(r["precinct_key"]).strip().upper(): str(r["vtd_geoid20"]).strip().upper()
        for _, r in top.iterrows()
    }


def _norm(text: str) -> str:
    return str(text).upper().strip()


def _compact(text: str) -> str:
    t = _norm(text)
    return "".join(ch for ch in t if ch.isalnum())


def _normalize_precinct_token(text: str) -> str:
    t = _norm(text)
    for word in COMMON_PRECINCT_WORDS:
        t = t.replace(word, " ")
    t = t.replace("-", " ").replace("_", " ").replace(".", " ")
    t = " ".join(t.split())
    return t


def _is_non_geographic_precinct(p: str) -> bool:
    t = _norm(p)
    flags = [
        "ABSENTEE",
        "PROVISIONAL",
        "CURBSIDE",
        "ONE STOP",
        "EARLY VOT",
        "TRANSFER",
        "MAIL",
        "STOP ",
        "EARLY ",
    ]
    if any(f in t for f in flags):
        return True
    # Countywide early-vote naming patterns in NC exports.
    # Examples: "EV CHL", "EV-WATKINS", "EV_POLL", "PITT - EV AG CENTER".
    return (
        t.startswith("OS-")
        or t.startswith("EV ")
        or t.startswith("EV-")
        or t.startswith("EV_")
        or " EV " in t
        or "-EV " in t
        or "_EV " in t
    )


def _extract_code_name_aliases(raw: str) -> list[str]:
    aliases = set()
    p = _norm(raw)
    pn = _normalize_precinct_token(raw)
    aliases.add(p)
    aliases.add(_compact(p))
    aliases.add(pn)
    aliases.add(_compact(pn))

    if "_" in p:
        code, name = p.split("_", 1)
        aliases.add(code.strip())
        aliases.add(name.strip())
        aliases.add(_compact(code))
        aliases.add(_compact(name))

    parts = pn.split()
    if parts:
        first = parts[0]
        if any(ch.isdigit() for ch in first):
            aliases.add(first)
            aliases.add(_compact(first))
            rest = " ".join(parts[1:]).strip()
            if rest:
                aliases.add(rest)
                aliases.add(_compact(rest))

    # Precinct code variants (01.1 vs 011 vs 0011 etc.)
    s = p.replace("-", ".")
    if "." in s:
        a, b = s.split(".", 1)
        if a.isdigit() and b.isdigit():
            aliases.add(f"{int(a)}.{int(b)}")
            aliases.add(f"{int(a):02d}.{int(b)}")
            aliases.add(f"{int(a):02d}{int(b)}")
            aliases.add(f"{int(a):02d}{int(b):02d}")
    if p.isdigit():
        aliases.add(str(int(p)))
        aliases.add(p.zfill(4))

    return [a for a in aliases if a]


def load_precinct_overrides(path: Path) -> dict[str, dict[str, str]]:
    """
    Load manual key overrides from CSV with columns:
      year,raw_precinct_key,canonical_precinct_key
    year can be blank or '*' to apply to all years.
    """
    out: dict[str, dict[str, str]] = defaultdict(dict)
    if not path.exists():
        return out
    df = pd.read_csv(path, dtype=str).fillna("")
    needed = {"year", "raw_precinct_key", "canonical_precinct_key"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"Override CSV missing columns: {sorted(needed - set(df.columns))}")
    for _, r in df.iterrows():
        year = _norm(r["year"]) or "*"
        raw_key = _norm(r["raw_precinct_key"])
        canonical = _norm(r["canonical_precinct_key"])
        if not raw_key or not canonical:
            continue
        out[year][raw_key] = canonical
    return out


NC_COUNTY_FIPS = {
    "001": "ALAMANCE", "003": "ALEXANDER", "005": "ALLEGHANY", "007": "ANSON",
    "009": "ASHE", "011": "AVERY", "013": "BEAUFORT", "015": "BERTIE",
    "017": "BLADEN", "019": "BRUNSWICK", "021": "BUNCOMBE", "023": "BURKE",
    "025": "CABARRUS", "027": "CALDWELL", "029": "CAMDEN", "031": "CARTERET",
    "033": "CASWELL", "035": "CATAWBA", "037": "CHATHAM", "039": "CHEROKEE",
    "041": "CHOWAN", "043": "CLAY", "045": "CLEVELAND", "047": "COLUMBUS",
    "049": "CRAVEN", "051": "CUMBERLAND", "053": "CURRITUCK", "055": "DARE",
    "057": "DAVIDSON", "059": "DAVIE", "061": "DUPLIN", "063": "DURHAM",
    "065": "EDGECOMBE", "067": "FORSYTH", "069": "FRANKLIN", "071": "GASTON",
    "073": "GATES", "075": "GRAHAM", "077": "GRANVILLE", "079": "GREENE",
    "081": "GUILFORD", "083": "HALIFAX", "085": "HARNETT", "087": "HAYWOOD",
    "089": "HENDERSON", "091": "HERTFORD", "093": "HOKE", "095": "HYDE",
    "097": "IREDELL", "099": "JACKSON", "101": "JOHNSTON", "103": "JONES",
    "105": "LEE", "107": "LENOIR", "109": "LINCOLN", "111": "MCDOWELL",
    "113": "MACON", "115": "MADISON", "117": "MARTIN", "119": "MECKLENBURG",
    "121": "MITCHELL", "123": "MONTGOMERY", "125": "MOORE", "127": "NASH",
    "129": "NEW HANOVER", "131": "NORTHAMPTON", "133": "ONSLOW", "135": "ORANGE",
    "137": "PAMLICO", "139": "PASQUOTANK", "141": "PENDER", "143": "PERQUIMANS",
    "145": "PERSON", "147": "PITT", "149": "POLK", "151": "RANDOLPH",
    "153": "RICHMOND", "155": "ROBESON", "157": "ROCKINGHAM", "159": "ROWAN",
    "161": "RUTHERFORD", "163": "SAMPSON", "165": "SCOTLAND", "167": "STANLY",
    "169": "STOKES", "171": "SURRY", "173": "SWAIN", "175": "TRANSYLVANIA",
    "177": "TYRRELL", "179": "UNION", "181": "VANCE", "183": "WAKE",
    "185": "WARREN", "187": "WASHINGTON", "189": "WATAUGA", "191": "WAYNE",
    "193": "WILKES", "195": "WILSON", "197": "YADKIN", "199": "YANCEY",
}


def build_precinct_alias_index(voting_geojson_path: Path) -> dict[str, dict[str, set[str]]]:
    geo = json.load(open(voting_geojson_path, "r", encoding="utf-8"))
    county_map: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for f in geo.get("features", []):
        props = f.get("properties", {})
        county = _norm(props.get("county_nam", ""))
        prec_id = _norm(props.get("prec_id", ""))
        enr_desc = _norm(props.get("enr_desc", ""))
        if not county or not prec_id:
            continue
        canonical = f"{county} - {prec_id}"

        aliases = set()
        aliases.update(_extract_code_name_aliases(prec_id))
        if enr_desc:
            aliases.update(_extract_code_name_aliases(enr_desc))
            aliases.update(_extract_code_name_aliases(f"{prec_id}_{enr_desc}"))
            aliases.update(_extract_code_name_aliases(f"{prec_id} {enr_desc}"))

        for a in aliases:
            county_map[county][a].add(canonical)

    return county_map


def _canonical_code_maps(alias_index: dict[str, dict[str, set[str]]]) -> dict[str, dict[str, set[str]]]:
    out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for county, aliases in alias_index.items():
        canonical_keys = set()
        for vals in aliases.values():
            canonical_keys.update(vals)
        for canon in canonical_keys:
            if " - " not in canon:
                continue
            _, precinct = canon.split(" - ", 1)
            out[county][_compact(precinct)].add(canon)
    return out


def _county_name_from_record(props: dict, county_col: str) -> str:
    raw = props.get(county_col, "")
    if raw is None:
        return ""
    s = str(raw).strip()
    if s.isdigit():
        return NC_COUNTY_FIPS.get(s.zfill(3), "")
    return _norm(s)


def enrich_alias_index_from_vtd(
    alias_index: dict[str, dict[str, set[str]]],
    *,
    vtd_path: Path,
    county_col: str,
    code_col: str,
    name_col: str,
) -> int:
    if not vtd_path.exists():
        return 0
    vtd = json.load(open(vtd_path, "r", encoding="utf-8")) if vtd_path.suffix.lower() == ".geojson" else None
    if vtd is None:
        import geopandas as gpd  # local import to avoid dependency at module import time

        gdf = gpd.read_file(vtd_path)
        features = [{"properties": row} for row in gdf.to_dict("records")]
    else:
        features = vtd.get("features", [])

    code_map = _canonical_code_maps(alias_index)
    added = 0

    for f in features:
        props = f.get("properties", {})
        county = _county_name_from_record(props, county_col)
        if not county or county not in alias_index:
            continue
        code = _norm(props.get(code_col, ""))
        name = _norm(props.get(name_col, ""))
        if not code:
            continue

        county_aliases = alias_index[county]
        candidates = set()

        for a in _extract_code_name_aliases(code):
            vals = county_aliases.get(a)
            if vals:
                candidates.update(vals)

        if name:
            for a in _extract_code_name_aliases(name):
                vals = county_aliases.get(a)
                if vals:
                    candidates.update(vals)

        code_compact = _compact(code)
        if code_compact in code_map[county]:
            candidates.update(code_map[county][code_compact])

        if len(candidates) != 1:
            continue
        canonical = next(iter(candidates))
        for a in _extract_code_name_aliases(code):
            if canonical not in county_aliases[a]:
                county_aliases[a].add(canonical)
                added += 1
        if name:
            for a in _extract_code_name_aliases(name):
                if canonical not in county_aliases[a]:
                    county_aliases[a].add(canonical)
                    added += 1

    return added


def resolve_precinct_key(
    election_precinct_key: str,
    alias_index: dict[str, dict[str, set[str]]],
) -> tuple[str | None, str]:
    # Returns (canonical_precinct_key, status)
    if " - " not in election_precinct_key:
        return None, "bad_key"
    county, precinct = election_precinct_key.split(" - ", 1)
    county = _norm(county)
    precinct = _norm(precinct)
    if _is_non_geographic_precinct(precinct):
        return None, "non_geographic"

    # Wake often embeds codes like "01-14" in strings like "PRECINCT 01-14A".
    # Normalize to the base code to match BAF/VTD crosswalk keys.
    if county == "WAKE":
        m = re.search(r"\b(\d{2}-\d{2})\b", precinct)
        if m:
            precinct = m.group(1)

    county_aliases = alias_index.get(county)
    if not county_aliases:
        return None, "no_county"

    cands = _extract_code_name_aliases(precinct)
    hits = set()
    for a in cands:
        vals = county_aliases.get(a)
        if vals:
            hits.update(vals)

    if len(hits) == 1:
        return next(iter(hits)), "matched"
    if len(hits) > 1:
        return None, "ambiguous"
    return None, "unmatched"


def allocate_office_results(
    office_results: dict,
    crosswalk: dict[str, list[tuple[str, float]]],
    alias_index: dict[str, dict[str, set[str]]],
    county_fallback: dict[str, list[tuple[str, float]]] | None = None,
    county_fallback_non_geo: dict[str, list[tuple[str, float]]] | None = None,
    county_fallback_legacy: dict[str, list[tuple[str, float]]] | None = None,
    precinct_to_vtd: dict[str, str] | None = None,
    year: str | None = None,
    overrides_by_year: dict[str, dict[str, str]] | None = None,
) -> tuple[dict, dict[str, int]]:
    by_district: dict[str, dict[str, float]] = defaultdict(
        lambda: {"dem_votes": 0.0, "rep_votes": 0.0, "other_votes": 0.0}
    )
    stats = defaultdict(int)
    rows: list[dict] = []

    for precinct_key, row in office_results.items():
        stats["total"] += 1
        key = str(precinct_key).strip().upper()

        # Explicit operator overrides take precedence.
        if overrides_by_year:
            yk = str(year) if year is not None else ""
            hit = (overrides_by_year.get(yk, {}).get(key)
                   or overrides_by_year.get("*", {}).get(key))
            if hit:
                key = hit
                stats["manual_override"] += 1

        resolved_key, status = resolve_precinct_key(key, alias_index)
        stats[status] += 1
        if resolved_key:
            key = resolved_key

        splits = crosswalk.get(key)
        if not splits and precinct_to_vtd:
            vtd_key = precinct_to_vtd.get(key)
            if vtd_key:
                splits = crosswalk.get(vtd_key)
                if splits:
                    stats["vtd_bridge"] += 1
        dem = float(row.get("dem_votes", 0) or 0)
        rep = float(row.get("rep_votes", 0) or 0)
        oth = float(row.get("other_votes", 0) or 0)
        county = key.split(" - ", 1)[0] if " - " in key else ""
        rows.append(
            {
                "key": key,
                "county": county,
                "status": status,
                "dem": dem,
                "rep": rep,
                "oth": oth,
                "splits": splits,
            }
        )

    # Build county-level district shares from already matched geographic precinct rows
    # so unresolved/early-vote buckets can be distributed by local voting pattern.
    county_dist_votes: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for rec in rows:
        if not rec["splits"]:
            continue
        if rec["status"] == "non_geographic":
            continue
        tot = rec["dem"] + rec["rep"] + rec["oth"]
        if tot <= 0 or not rec["county"]:
            continue
        for district, weight in rec["splits"]:
            county_dist_votes[rec["county"]][district] += tot * float(weight)

    county_dynamic_fallback: dict[str, list[tuple[str, float]]] = {}
    for county, dmap in county_dist_votes.items():
        total = sum(dmap.values())
        if total <= 0:
            continue
        county_dynamic_fallback[county] = sorted(
            [(d, v / total) for d, v in dmap.items()],
            key=lambda x: x[1],
            reverse=True,
        )

    for rec in rows:
        splits = rec["splits"]
        county = rec["county"]
        status = rec["status"]
        if not splits:
            if (
                county
                and status in {"non_geographic", "unmatched", "ambiguous", "no_county", "bad_key"}
                and county in county_dynamic_fallback
            ):
                splits = county_dynamic_fallback[county]
                stats["county_fallback_dynamic"] += 1
            if (
                not splits
                and county
                and status == "non_geographic"
                and county_fallback_non_geo
                and county in county_fallback_non_geo
            ):
                splits = county_fallback_non_geo[county]
                stats["county_fallback_non_geo"] += 1
            if not splits and county and county_fallback and county in county_fallback:
                splits = county_fallback[county]
                stats["county_fallback"] += 1
            # Legacy safety net: for older cycles, prefer preventing dropped votes.
            if not splits and county and county_fallback_legacy:
                try:
                    year_int = int(year) if year is not None else None
                except (TypeError, ValueError):
                    year_int = None
                if (
                    year_int is not None
                    and year_int <= 2020
                    and status in {"unmatched", "ambiguous", "no_county", "bad_key"}
                    and county in county_fallback_legacy
                ):
                    splits = county_fallback_legacy[county]
                    stats["county_fallback_legacy"] += 1
        if not splits:
            continue
        stats["crosswalk_matched"] += 1

        for district, weight in splits:
            by_district[district]["dem_votes"] += rec["dem"] * weight
            by_district[district]["rep_votes"] += rec["rep"] * weight
            by_district[district]["other_votes"] += rec["oth"] * weight

    out = {}
    for district, vals in by_district.items():
        dem = int(round(vals["dem_votes"]))
        rep = int(round(vals["rep_votes"]))
        oth = int(round(vals["other_votes"]))
        total_votes = dem + rep + oth
        margin = rep - dem
        margin_pct = (margin / total_votes * 100) if total_votes else 0.0
        winner = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        out[district] = {
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": oth,
            "total_votes": total_votes,
            "dem_candidate": "",
            "rep_candidate": "",
            "margin": margin,
            "margin_pct": round(margin_pct, 2),
            "winner": winner,
            "competitiveness": {"color": calculate_competitiveness(margin_pct)},
        }
    return out, stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reallocate precinct election results to current NC district lines."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input aggregated election JSON (default: data/nc_elections_aggregated.json).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output district results JSON (default: data/nc_district_results_2022_lines.json).",
    )
    parser.add_argument("--min-year", type=int, default=None, help="Inclusive minimum year filter.")
    parser.add_argument("--max-year", type=int, default=None, help="Inclusive maximum year filter.")
    parser.add_argument(
        "--crosswalk-mode",
        choices=["precinct", "vtd"],
        default="precinct",
        help="Use precinct-key crosswalks (default) or VTD20 crosswalks with precinct->VTD bridge.",
    )
    parser.add_argument(
        "--overrides",
        type=Path,
        default=None,
        help="Optional overrides CSV (year,raw_precinct_key,canonical_precinct_key).",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data"
    in_json = args.input if args.input else (data_dir / "nc_elections_aggregated.json")
    if args.crosswalk_mode == "vtd":
        house_cw = data_dir / "crosswalks" / "vtd20_to_2024_state_house.csv"
        senate_cw = data_dir / "crosswalks" / "vtd20_to_2024_state_senate.csv"
        congress_cw = data_dir / "crosswalks" / "vtd20_to_cd118.csv"
        crosswalk_key_col = "vtd_geoid20"
    else:
        house_cw = data_dir / "crosswalks" / "precinct_to_2022_state_house.csv"
        senate_cw = data_dir / "crosswalks" / "precinct_to_2022_state_senate.csv"
        congress_cw = data_dir / "crosswalks" / "precinct_to_cd118.csv"
        crosswalk_key_col = "precinct_key"
    voting_geojson = data_dir / "Voting_Precincts.geojson"
    vtd_2008 = data_dir / "census" / "tl_2008_37_vtd00_merged.geojson"
    vtd_2012 = data_dir / "census" / "tl_2012_37_vtd10" / "tl_2012_37_vtd10.shp"
    vtd_2020 = (data_dir / "tl_2020_37_vtd20" / "tl_2020_37_vtd20.shp")
    if not vtd_2020.exists():
        vtd_2020 = data_dir / "census" / "tl_2020_37_vtd20" / "tl_2020_37_vtd20.shp"

    if not in_json.exists():
        raise FileNotFoundError(f"Missing {in_json}")
    if not house_cw.exists() or not senate_cw.exists() or not congress_cw.exists():
        raise FileNotFoundError("Missing precinct crosswalk CSVs. Run build_precinct_crosswalks_to_2024.py first.")

    src = json.load(open(in_json, "r", encoding="utf-8"))
    overrides_path = args.overrides if args.overrides else (data_dir / "mappings" / "precinct_key_overrides.csv")
    overrides_by_year = load_precinct_overrides(overrides_path)
    if overrides_by_year:
        total_overrides = sum(len(v) for v in overrides_by_year.values())
        print(f"Loaded manual overrides: {total_overrides} from {overrides_path}")
    house_map = load_crosswalk(house_cw, key_col=crosswalk_key_col)
    senate_map = load_crosswalk(senate_cw, key_col=crosswalk_key_col)
    congress_map = load_crosswalk(congress_cw, key_col=crosswalk_key_col)
    if args.crosswalk_mode == "vtd":
        house_county_fallback = None
        senate_county_fallback = None
        congress_county_fallback = None
        house_county_non_geo_fallback = None
        senate_county_non_geo_fallback = None
        congress_county_non_geo_fallback = None
        house_county_legacy_fallback = None
        senate_county_legacy_fallback = None
        congress_county_legacy_fallback = None
    else:
        house_county_fallback = build_county_fallback_map(house_cw)
        senate_county_fallback = build_county_fallback_map(senate_cw)
        congress_county_fallback = build_county_fallback_map(congress_cw)
        house_county_non_geo_fallback = build_county_fallback_map(house_cw, dominant_threshold=None)
        senate_county_non_geo_fallback = build_county_fallback_map(senate_cw, dominant_threshold=None)
        congress_county_non_geo_fallback = build_county_fallback_map(congress_cw, dominant_threshold=None)
        house_county_legacy_fallback = house_county_non_geo_fallback
        senate_county_legacy_fallback = senate_county_non_geo_fallback
        congress_county_legacy_fallback = congress_county_non_geo_fallback
    precinct_to_vtd = None
    if args.crosswalk_mode == "vtd":
        precinct_to_vtd = build_precinct_to_vtd_map(voting_geojson=voting_geojson, vtd_path=vtd_2020)
        print(f"Built precinct->VTD bridge: {len(precinct_to_vtd):,} precinct keys")
    alias_index = build_precinct_alias_index(voting_geojson)
    added_2008 = enrich_alias_index_from_vtd(
        alias_index,
        vtd_path=vtd_2008,
        county_col="COUNTYFP00",
        code_col="VTDST00",
        name_col="NAME00",
    )
    added_2012 = enrich_alias_index_from_vtd(
        alias_index,
        vtd_path=vtd_2012,
        county_col="COUNTYFP10",
        code_col="VTDST10",
        name_col="NAME10",
    )
    added_2020 = enrich_alias_index_from_vtd(
        alias_index,
        vtd_path=vtd_2020,
        county_col="COUNTYFP20",
        code_col="VTDST20",
        name_col="NAME20",
    )
    print(
        f"Alias enrichment added mappings: 2008={added_2008}, "
        f"2012={added_2012}, 2020={added_2020}"
    )

    dst = {
        "plan": {"id": PLAN_ID, "label": PLAN_LABEL},
        "results_by_year": {},
    }

    for year, year_data in src.get("results_by_year", {}).items():
        year_int = int(year)
        if args.min_year is not None and year_int < args.min_year:
            continue
        if args.max_year is not None and year_int > args.max_year:
            continue
        dst["results_by_year"][year] = {"state_house": {}, "state_senate": {}, "congressional": {}}
        for office_key, office_data in year_data.items():
            office_results = office_data.get("general", {}).get("results", {})
            if not office_results:
                continue

            house_results, hstats = allocate_office_results(
                office_results,
                house_map,
                alias_index,
                house_county_fallback,
                house_county_non_geo_fallback,
                house_county_legacy_fallback,
                precinct_to_vtd,
                year,
                overrides_by_year,
            )
            senate_results, sstats = allocate_office_results(
                office_results,
                senate_map,
                alias_index,
                senate_county_fallback,
                senate_county_non_geo_fallback,
                senate_county_legacy_fallback,
                precinct_to_vtd,
                year,
                overrides_by_year,
            )
            congress_results, cstats = allocate_office_results(
                office_results,
                congress_map,
                alias_index,
                congress_county_fallback,
                congress_county_non_geo_fallback,
                congress_county_legacy_fallback,
                precinct_to_vtd,
                year,
                overrides_by_year,
            )

            hcov = (hstats["crosswalk_matched"] / hstats["total"] * 100.0) if hstats["total"] else 0.0
            scov = (sstats["crosswalk_matched"] / sstats["total"] * 100.0) if sstats["total"] else 0.0
            ccov = (cstats["crosswalk_matched"] / cstats["total"] * 100.0) if cstats["total"] else 0.0

            dst["results_by_year"][year]["state_house"][office_key] = {
                "meta": {
                    "plan_id": PLAN_ID,
                    "plan_label": PLAN_LABEL,
                    "match_coverage_pct": round(hcov, 2),
                    "matched_precinct_keys": int(hstats["crosswalk_matched"]),
                    "total_precinct_keys": int(hstats["total"]),
                },
                "general": {"results": house_results},
            }
            dst["results_by_year"][year]["state_senate"][office_key] = {
                "meta": {
                    "plan_id": PLAN_ID,
                    "plan_label": PLAN_LABEL,
                    "match_coverage_pct": round(scov, 2),
                    "matched_precinct_keys": int(sstats["crosswalk_matched"]),
                    "total_precinct_keys": int(sstats["total"]),
                },
                "general": {"results": senate_results},
            }
            dst["results_by_year"][year]["congressional"][office_key] = {
                "meta": {
                    "plan_id": PLAN_ID,
                    "plan_label": PLAN_LABEL,
                    "match_coverage_pct": round(ccov, 2),
                    "matched_precinct_keys": int(cstats["crosswalk_matched"]),
                    "total_precinct_keys": int(cstats["total"]),
                },
                "general": {"results": congress_results},
            }
            print(
                f"{year} {office_key}: matched precinct keys -> "
                f"house {hstats['crosswalk_matched']}/{hstats['total']} ({hcov:.1f}%, vtd bridge {hstats.get('vtd_bridge', 0)}, county fb {hstats.get('county_fallback', 0)}, county non-geo fb {hstats.get('county_fallback_non_geo', 0)}, legacy fb {hstats.get('county_fallback_legacy', 0)}), "
                f"senate {sstats['crosswalk_matched']}/{sstats['total']} ({scov:.1f}%, vtd bridge {sstats.get('vtd_bridge', 0)}, county fb {sstats.get('county_fallback', 0)}, county non-geo fb {sstats.get('county_fallback_non_geo', 0)}, legacy fb {sstats.get('county_fallback_legacy', 0)}), "
                f"cd118 {cstats['crosswalk_matched']}/{cstats['total']} ({ccov:.1f}%, vtd bridge {cstats.get('vtd_bridge', 0)}, county fb {cstats.get('county_fallback', 0)}, county non-geo fb {cstats.get('county_fallback_non_geo', 0)}, legacy fb {cstats.get('county_fallback_legacy', 0)})"
            )

    out_json = args.output if args.output else (data_dir / "nc_district_results_2022_lines.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(dst, f, indent=2)
    print(f"\nWrote {out_json}")


if __name__ == "__main__":
    main()
