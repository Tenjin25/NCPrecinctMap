"""
Build 2024 district contest slices with true DEM/REP/OTHER allocation.

Pipeline:
1) Start from precinct-sort style rows (county, precinct, office, party, candidate, votes).
2) Reallocate non-geographic precinct rows (ABSENTEE/ONE-STOP/EARLY/etc.) to geographic
   precincts by candidate-performance shares within county.
3) Aggregate to precinct-level DEM/REP/OTHER.
4) VAP-shatter precinct totals to block-level, then aggregate to district scopes.
5) Emit data/district_contests/{scope}_{contest_type}_{year}.json + manifest.json.
"""
from __future__ import annotations

import argparse
import json
import re
from decimal import Decimal
from pathlib import Path

import pandas as pd

from shatter_precinct_votes_vap import aggregate_to_districts, load_crosswalk, load_vap, shatter_votes


NON_GEO_FLAGS = [
    "ABSENTEE",
    "ABSEN",
    "ABS",
    "ONE STOP",
    "ONE-STOP",
    "EARLY",
    "EV ",
    "EV-",
    "EV_",
    "PROVISIONAL",
    "PROVI",
    "PROV",
    "CURBSIDE",
    "MAIL",
]

KNOWN_OFFICE_KEYS = {
    "US PRESIDENT": "president",
    "PRESIDENT": "president",
    "PRESIDENT-VICE PRESIDENT": "president",
    "PRESIDENT AND VICE PRESIDENT": "president",
    "PRESIDENT-VICE-PRESIDENT": "president",
    "US SENATE": "us_senate",
    "UNITED STATES SENATE": "us_senate",
    "NC GOVERNOR": "governor",
    "GOVERNOR": "governor",
    "NC LIEUTENANT GOVERNOR": "lieutenant_governor",
    "LIEUTENANT GOVERNOR": "lieutenant_governor",
    "NC ATTORNEY GENERAL": "attorney_general",
    "ATTORNEY GENERAL": "attorney_general",
    "NC AUDITOR": "auditor",
    "AUDITOR": "auditor",
    "NC COMMISSIONER OF AGRICULTURE": "agriculture_commissioner",
    "COMMISSIONER OF AGRICULTURE": "agriculture_commissioner",
    "NC COMMISSIONER OF LABOR": "labor_commissioner",
    "COMMISSIONER OF LABOR": "labor_commissioner",
    "NC COMMISSIONER OF INSURANCE": "insurance_commissioner",
    "COMMISSIONER OF INSURANCE": "insurance_commissioner",
    "NC SECRETARY OF STATE": "secretary_of_state",
    "SECRETARY OF STATE": "secretary_of_state",
    "NC TREASURER": "treasurer",
    "TREASURER": "treasurer",
    "NC SUPERINTENDENT OF PUBLIC INSTRUCTION": "superintendent",
    "SUPERINTENDENT OF PUBLIC INSTRUCTION": "superintendent",
    "NC COURT OF APPEALS JUDGE SEAT 12": "nc_court_of_appeals_judge_seat_12",
    "NC COURT OF APPEALS JUDGE SEAT 14": "nc_court_of_appeals_judge_seat_14",
    "NC COURT OF APPEALS JUDGE SEAT 15": "nc_court_of_appeals_judge_seat_15",
    "NC SUPREME COURT ASSOCIATE JUSTICE SEAT 06": "nc_supreme_court_associate_justice_seat_06",
}

PRESIDENT_OFFICE_KEY = "president"


def normalize_presidential_candidate_name(name: str) -> str:
    """
    Strip running mate / ticket formatting from presidential candidate strings.
    Examples:
      "DONALD J. TRUMP / J.D. VANCE" -> "DONALD J. TRUMP"
      "A. Gore-J. Lieberman" -> "A. Gore"
    """
    raw = str(name or "").strip()
    if not raw:
        return ""

    for sep in [" / ", "/", " & ", "&", " + ", "+", " - ", " – ", " — "]:
        if sep in raw:
            left = raw.split(sep, 1)[0].strip()
            return left if left else raw

    # Hyphen tickets in older datasets, but try not to mangle compound surnames.
    if "-" in raw:
        left, right = raw.split("-", 1)
        left = left.strip()
        right = right.strip()
        if left and right and (("." in right) or (" " in right) or (right.isupper() and len(right) <= 20)):
            return left

    return raw


def infer_office_key(office: str) -> str | None:
    o_full = str(office).strip().upper()
    o_full = re.sub(r"\s+", " ", o_full)

    # Strip common parenthetical metadata like "(VOTE FOR 1)" but keep named-seat labels.
    o = re.sub(r"\s+\((?:VOTE FOR|VOTE|NONPARTISAN|PARTISAN|UNEXPIRED).*?\)$", "", o_full)

    direct = KNOWN_OFFICE_KEYS.get(o)
    if direct:
        return direct

    def _slug(s: str) -> str:
        s = str(s).strip().upper()
        s = re.sub(r"[^A-Z0-9]+", "_", s)
        s = s.strip("_")
        return s.lower()

    m = re.match(r"^NC COURT OF APPEALS JUDGE SEAT\s*0*([0-9]+)$", o)
    if m:
        return f"nc_court_of_appeals_judge_seat_{int(m.group(1)):02d}"

    m = re.match(r"^NC SUPREME COURT ASSOCIATE JUSTICE SEAT\s*0*([0-9]+)$", o)
    if m:
        return f"nc_supreme_court_associate_justice_seat_{int(m.group(1)):02d}"

    m = re.match(r"^NC SUPREME COURT CHIEF JUSTICE SEAT\s*0*([0-9]+)$", o)
    if m:
        return f"nc_supreme_court_chief_justice_seat_{int(m.group(1)):02d}"

    if o == "NC SUPREME COURT CHIEF JUSTICE":
        return "nc_supreme_court_chief_justice"

    # Legacy presidential label.
    if "PRESIDENT" in o_full and "VICE PRESIDENT" in o_full and "REPRESENTATIVES" not in o_full:
        return "president"

    # Older NCSBE labels often used named seats, e.g.:
    #   "SUPREME COURT ASSOCIATE JUSTICE (EDMUNDS SEAT)"
    #   "COURT OF APPEALS JUDGE (TYSON SEAT)"
    m = re.match(r"^SUPREME COURT ASSOCIATE JUSTICE\s*\((.+?)\s+SEAT\)$", o_full)
    if m:
        return f"nc_supreme_court_associate_justice_{_slug(m.group(1))}_seat"

    m = re.match(r"^SUPREME COURT CHIEF JUSTICE\s*\((.+?)\s+SEAT\)$", o_full)
    if m:
        return f"nc_supreme_court_chief_justice_{_slug(m.group(1))}_seat"

    m = re.match(r"^COURT OF APPEALS JUDGE\s*\((.+?)\s+SEAT\)$", o_full)
    if m:
        return f"nc_court_of_appeals_judge_{_slug(m.group(1))}_seat"

    # Alternate legacy formats using a dash instead of parentheses.
    m = re.match(r"^SUPREME COURT ASSOCIATE JUSTICE\s*-\s*(.+?)\s+SEAT$", o_full)
    if m:
        return f"nc_supreme_court_associate_justice_{_slug(m.group(1))}_seat"

    m = re.match(r"^SUPREME COURT CHIEF JUSTICE\s*-\s*(.+?)\s+SEAT$", o_full)
    if m:
        return f"nc_supreme_court_chief_justice_{_slug(m.group(1))}_seat"

    m = re.match(r"^COURT OF APPEALS JUDGE\s*-\s*(.+?)\s+SEAT$", o_full)
    if m:
        return f"nc_court_of_appeals_judge_{_slug(m.group(1))}_seat"

    # 2014-ish formats like:
    #   "NC COURT OF APPEALS JUDGE (DAVIS)"
    #   "NC SUPREME COURT ASSOCIATE JUSTICE (HUDSON)"
    #   "NC SUPREME COURT CHIEF JUSTICE (PARKER)"
    m = re.match(r"^NC COURT OF APPEALS JUDGE\s*\((.+?)\)$", o_full)
    if m:
        return f"nc_court_of_appeals_judge_{_slug(m.group(1))}_seat"

    m = re.match(r"^NC SUPREME COURT ASSOCIATE JUSTICE\s*\((.+?)\)$", o_full)
    if m:
        return f"nc_supreme_court_associate_justice_{_slug(m.group(1))}_seat"

    m = re.match(r"^NC SUPREME COURT CHIEF JUSTICE\s*\((.+?)\)$", o_full)
    if m:
        return f"nc_supreme_court_chief_justice_{_slug(m.group(1))}_seat"

    return None


def is_non_geographic_precinct(name: str) -> bool:
    t = str(name).strip().upper()
    if re.match(r"^EV[A-Z0-9]+$", t):
        return True
    return any(flag in t for flag in NON_GEO_FLAGS)


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


def load_district_map(path: Path, block_col: str, district_col: str) -> pd.DataFrame:
    d = pd.read_csv(path, dtype=str)
    d.columns = [str(c).strip() for c in d.columns]
    out = d[[block_col, district_col]].copy()
    out.columns = ["block_geoid20", "district"]
    out["block_geoid20"] = out["block_geoid20"].astype(str).str.strip().str.zfill(15)
    out["district"] = out["district"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    m = out["district"].str.match(r"^\d+$", na=False)
    out.loc[m, "district"] = out.loc[m, "district"].str.lstrip("0")
    out.loc[out["district"] == "", "district"] = "0"
    return out.dropna().drop_duplicates(subset=["block_geoid20"], keep="first")


def build_county_shares(
    crosswalk_df: pd.DataFrame,
    vap_df: pd.DataFrame,
    district_map: pd.DataFrame,
) -> pd.DataFrame:
    cw = crosswalk_df.copy()
    cw["county"] = cw["precinct_id"].astype(str).str.split(" - ").str[0].str.strip().str.upper()
    v = vap_df.copy()
    v["vap_count"] = pd.to_numeric(v["vap_count"], errors="coerce").fillna(0.0)
    m = (
        cw[["block_geoid20", "county"]]
        .merge(v[["block_geoid20", "vap_count"]], on="block_geoid20", how="left")
        .merge(district_map[["block_geoid20", "district"]], on="block_geoid20", how="inner")
    )
    m["vap_count"] = m["vap_count"].fillna(0.0)
    g = m.groupby(["county", "district"], as_index=False)["vap_count"].sum()
    den = g.groupby("county", as_index=False)["vap_count"].sum().rename(columns={"vap_count": "county_vap"})
    g = g.merge(den, on="county", how="left")
    g["share"] = g["vap_count"] / g["county_vap"]
    return g[["county", "district", "share"]]


def build_precinct_bucket_shares(
    crosswalk_df: pd.DataFrame,
    vap_df: pd.DataFrame,
    district_map: pd.DataFrame,
) -> pd.DataFrame:
    cw = crosswalk_df.copy()
    cw["county"] = cw["precinct_id"].astype(str).str.split(" - ").str[0].str.strip().str.upper()
    p = cw["precinct_id"].astype(str).str.split(" - ").str[1].fillna("").str.strip().str.upper()
    cw["bucket"] = p.str.split("-").str[0].str.strip()
    cw = cw[cw["bucket"] != ""].copy()

    v = vap_df.copy()
    v["vap_count"] = pd.to_numeric(v["vap_count"], errors="coerce").fillna(0.0)
    m = (
        cw[["block_geoid20", "county", "bucket"]]
        .merge(v[["block_geoid20", "vap_count"]], on="block_geoid20", how="left")
        .merge(district_map[["block_geoid20", "district"]], on="block_geoid20", how="inner")
    )
    m["vap_count"] = m["vap_count"].fillna(0.0)
    g = m.groupby(["county", "bucket", "district"], as_index=False)["vap_count"].sum()
    den = g.groupby(["county", "bucket"], as_index=False)["vap_count"].sum().rename(columns={"vap_count": "bucket_vap"})
    g = g.merge(den, on=["county", "bucket"], how="left")
    g["share"] = g["vap_count"] / g["bucket_vap"]
    return g[["county", "bucket", "district", "share"]]


def load_allocation_weights(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_precinct_overrides(path: Path, year: int) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return {}
    if df.empty:
        return {}
    req = {"raw_precinct_key", "canonical_precinct_key"}
    if not req.issubset(set(df.columns)):
        return {}
    if "year" in df.columns:
        y = str(int(year))
        df = df[(df["year"].astype(str).str.strip() == "") | (df["year"].astype(str).str.strip() == y)].copy()
    df["raw_precinct_key"] = df["raw_precinct_key"].astype(str).str.strip().str.upper()
    df["canonical_precinct_key"] = df["canonical_precinct_key"].astype(str).str.strip().str.upper()
    df = df[(df["raw_precinct_key"] != "") & (df["canonical_precinct_key"] != "")]
    return dict(zip(df["raw_precinct_key"], df["canonical_precinct_key"]))


def build_auto_precinct_overrides(precinct_ids: pd.Series, matched_precincts: set[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    vals = set(precinct_ids.astype(str).str.strip().str.upper())
    for raw in sorted(vals):
        if not raw or raw in matched_precincts or " - " not in raw:
            continue
        county, p = raw.split(" - ", 1)
        county = county.strip()
        p = p.strip()

        # Example: WAKE - 01-07A -> WAKE - 01-07 if canonical exists.
        if p.endswith("A"):
            cand = f"{county} - {p[:-1]}"
            if cand in matched_precincts:
                out[raw] = cand
                continue

        # Example: GASTON - 29-1 -> GASTON - 29A; GASTON - 04-1 -> GASTON - 4A.
        m = re.match(r"^0*([0-9]+)(?:-1)?$", p)
        if m:
            cand = f"{county} - {int(m.group(1))}A"
            if cand in matched_precincts:
                out[raw] = cand
                continue
    return out


def apply_precinct_overrides(df: pd.DataFrame, overrides: dict[str, str] | None) -> pd.DataFrame:
    if not overrides:
        return df
    out = df.copy()
    out["precinct_id"] = out["precinct_id"].astype(str).str.strip().str.upper()
    out["precinct_id"] = out["precinct_id"].map(lambda k: overrides.get(k, k))
    return out


def apply_county_share_overrides(
    county_shares: pd.DataFrame,
    *,
    year: int,
    scope: str,
    allocation_weights: dict,
    min_county_share: float = 0.0,
) -> pd.DataFrame:
    out = county_shares.copy()
    inserts = []
    scope_weights = (allocation_weights.get(str(int(year)), {}) or {}).get(str(scope), {}) or {}
    for county, weights in scope_weights.items():
        county_u = str(county).strip().upper()
        out = out[out["county"].astype(str).str.upper() != county_u].copy()
        raw = {str(k).strip(): float(v) for k, v in weights.items()}
        if min_county_share > 0:
            raw = {k: v for k, v in raw.items() if v >= float(min_county_share)}
        if not raw:
            continue
        total = sum(raw.values())
        if total <= 0:
            continue
        for district, share in raw.items():
            inserts.append(
                {
                    "county": county_u,
                    "district": str(district).strip(),
                    "share": float(share) / total,
                }
            )
    if inserts:
        out = pd.concat([out, pd.DataFrame(inserts)], ignore_index=True)
    return out


def apply_unmatched_county_fallback(
    district_df: pd.DataFrame,
    results_df: pd.DataFrame,
    matched_precincts: set[str],
    county_shares: pd.DataFrame,
    precinct_bucket_shares: pd.DataFrame | None = None,
) -> dict[str, int]:
    d = district_df.copy()
    d["district"] = d["district"].astype(str).str.strip()
    d["votes_rounded"] = pd.to_numeric(d["votes_rounded"], errors="coerce").fillna(0.0)
    base = d.set_index("district")["votes_rounded"].to_dict()

    r = results_df.copy()
    r["precinct_id"] = r["precinct_id"].astype(str).str.strip().str.upper()
    r["votes"] = pd.to_numeric(r["votes"], errors="coerce").fillna(0.0)
    r["county"] = r["precinct_id"].str.split(" - ").str[0].str.strip().str.upper()
    r["precinct"] = r["precinct_id"].str.split(" - ").str[1].fillna("").str.strip().str.upper()
    r["bucket"] = r["precinct"].str.split("-").str[0].str.strip()
    unmatched = r[~r["precinct_id"].isin(matched_precincts)].copy()
    if unmatched.empty:
        return {str(k): int(round(v)) for k, v in base.items()}

    add_frames = []
    assigned = pd.DataFrame(columns=["county", "bucket"])
    if precinct_bucket_shares is not None and not precinct_bucket_shares.empty:
        u_bucket = unmatched.groupby(["county", "bucket"], as_index=False)["votes"].sum().rename(
            columns={"votes": "unmatched_votes"}
        )
        b_alloc = u_bucket.merge(precinct_bucket_shares, on=["county", "bucket"], how="inner")
        if not b_alloc.empty:
            b_alloc["alloc_votes"] = b_alloc["unmatched_votes"] * b_alloc["share"]
            add_frames.append(b_alloc[["district", "alloc_votes"]])
            assigned = b_alloc[["county", "bucket"]].drop_duplicates()

    rem = unmatched
    if not assigned.empty:
        rem = unmatched.merge(assigned, on=["county", "bucket"], how="left", indicator=True)
        rem = rem[rem["_merge"] == "left_only"].drop(columns=["_merge"])

    if not rem.empty:
        u = rem.groupby("county", as_index=False)["votes"].sum().rename(columns={"votes": "unmatched_votes"})
        alloc = u.merge(county_shares, on="county", how="left").dropna(subset=["district", "share"]).copy()
        alloc["alloc_votes"] = alloc["unmatched_votes"] * alloc["share"]
        add_frames.append(alloc[["district", "alloc_votes"]])

    if not add_frames:
        return {str(k): int(round(v)) for k, v in base.items()}

    add = pd.concat(add_frames, ignore_index=True).groupby("district", as_index=False)["alloc_votes"].sum()
    for _, row in add.iterrows():
        dist = str(row["district"]).strip()
        base[dist] = float(base.get(dist, 0.0)) + float(row["alloc_votes"])
    return {str(k): int(round(v)) for k, v in base.items()}


def party_group(party: str) -> str:
    p = str(party).strip().upper()
    if p == "DEM":
        return "dem_votes"
    if p == "REP":
        return "rep_votes"
    return "other_votes"


def apply_candidate_party_overrides(df: pd.DataFrame, election_year: int | None = None) -> pd.DataFrame:
    """
    Targeted overrides for known edge cases where ballot-party label should not
    be treated as DEM/REP for margin calculations.
    """
    out = df.copy()
    if out.empty:
        return out

    y = None
    try:
        if election_year is not None:
            y = int(election_year)
    except Exception:
        y = None

    if y == 2018:
        cand = out["candidate"].astype(str).str.upper()
        office = out["office"].astype(str).str.upper()
        # Chris/Christopher Anglin in NC Supreme Court race should roll into Other.
        mask = cand.str.contains(r"\bANGLIN\b", regex=True, na=False) & office.str.contains("SUPREME COURT", na=False)
        out.loc[mask, "party_group"] = "other_votes"

    return out


def allocate_non_geo_by_candidate(
    df_office: pd.DataFrame, precinct_overrides: dict[str, str] | None = None
) -> pd.DataFrame:
    """
    Returns rows at county+precinct+candidate with votes after non-geo allocation.
    """
    df = df_office.copy()
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0.0)
    df["county"] = df["county"].astype(str).str.strip().str.upper()
    df["precinct"] = df["precinct"].astype(str).str.strip().str.upper()
    df["candidate"] = df["candidate"].astype(str).str.strip()
    df["precinct_id"] = df["county"] + " - " + df["precinct"]
    df = apply_precinct_overrides(df, precinct_overrides)
    df["non_geo"] = df["precinct"].map(is_non_geographic_precinct)

    geo = df[~df["non_geo"]].copy()
    non_geo = df[df["non_geo"]].copy()
    if non_geo.empty:
        return geo.groupby(["county", "precinct_id", "candidate"], as_index=False)["votes"].sum()

    geo_cand = geo.groupby(["county", "candidate", "precinct_id"], as_index=False)["votes"].sum()
    cand_den = geo_cand.groupby(["county", "candidate"], as_index=False)["votes"].sum().rename(
        columns={"votes": "cand_geo_total"}
    )
    non_geo_cand = non_geo.groupby(["county", "candidate"], as_index=False)["votes"].sum().rename(
        columns={"votes": "non_geo_votes"}
    )

    alloc = geo_cand.merge(cand_den, on=["county", "candidate"], how="left").merge(
        non_geo_cand, on=["county", "candidate"], how="left"
    )
    alloc["non_geo_votes"] = alloc["non_geo_votes"].fillna(0.0)
    alloc["alloc"] = 0.0
    ok = alloc["cand_geo_total"] > 0
    alloc.loc[ok, "alloc"] = alloc.loc[ok, "non_geo_votes"] * (
        alloc.loc[ok, "votes"] / alloc.loc[ok, "cand_geo_total"]
    )

    miss = non_geo_cand.merge(cand_den, on=["county", "candidate"], how="left")
    miss = miss[(miss["cand_geo_total"].isna()) & (miss["non_geo_votes"] > 0)].copy()
    if not miss.empty:
        county_geo = geo.groupby(["county", "precinct_id"], as_index=False)["votes"].sum()
        county_den = county_geo.groupby("county", as_index=False)["votes"].sum().rename(columns={"votes": "county_geo_total"})
        cshare = county_geo.merge(county_den, on="county", how="left")
        cshare["share"] = cshare["votes"] / cshare["county_geo_total"]
        miss_alloc = miss.merge(cshare[["county", "precinct_id", "share"]], on="county", how="left")
        miss_alloc["alloc"] = miss_alloc["non_geo_votes"] * miss_alloc["share"].fillna(0.0)
        alloc_extra = miss_alloc.groupby(["county", "precinct_id"], as_index=False)["alloc"].sum()
    else:
        alloc_extra = pd.DataFrame(columns=["county", "precinct_id", "alloc"])

    alloc_main = alloc.groupby(["county", "precinct_id"], as_index=False)["alloc"].sum()
    alloc_all = pd.concat([alloc_main, alloc_extra], ignore_index=True).groupby(
        ["county", "precinct_id"], as_index=False
    )["alloc"].sum()

    geo_tot = geo.groupby(["county", "precinct_id", "candidate"], as_index=False)["votes"].sum()
    # Add candidate-specific allocation where available.
    merged = geo_tot.merge(alloc[["county", "precinct_id", "candidate", "alloc"]], on=["county", "precinct_id", "candidate"], how="left")
    merged["alloc"] = merged["alloc"].fillna(0.0)
    merged["votes"] = merged["votes"] + merged["alloc"]

    # County-level fallback allocations were candidate-agnostic; distribute proportionally
    # across candidates in each precinct by existing geo candidate shares.
    if not alloc_extra.empty:
        p_cand = merged.groupby(["county", "precinct_id", "candidate"], as_index=False)["votes"].sum()
        p_tot = p_cand.groupby(["county", "precinct_id"], as_index=False)["votes"].sum().rename(columns={"votes": "p_total"})
        p_share = p_cand.merge(p_tot, on=["county", "precinct_id"], how="left")
        p_share["share"] = p_share["votes"] / p_share["p_total"]
        add = alloc_extra.merge(p_share[["county", "precinct_id", "candidate", "share"]], on=["county", "precinct_id"], how="left")
        add["votes_add"] = add["alloc"] * add["share"].fillna(0.0)
        add = add.groupby(["county", "precinct_id", "candidate"], as_index=False)["votes_add"].sum()
        merged = merged.merge(add, on=["county", "precinct_id", "candidate"], how="left")
        merged["votes_add"] = merged["votes_add"].fillna(0.0)
        merged["votes"] = merged["votes"] + merged["votes_add"]

    return merged[["county", "precinct_id", "candidate", "votes"]]


def build_precinct_party_votes(
    src: pd.DataFrame, office: str, precinct_overrides: dict[str, str] | None = None, election_year: int | None = None
) -> tuple[pd.DataFrame, str, str]:
    df = src[src["office"] == office].copy()
    if df.empty:
        return pd.DataFrame(columns=["precinct_id", "dem_votes", "rep_votes", "other_votes"]), "", ""
    df["votes_num"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0.0)
    df["party_group"] = df["party"].map(party_group)
    df = apply_candidate_party_overrides(df, election_year=election_year)

    # Candidate labels (statewide top by party).
    dem_c = (
        df[df["party_group"] == "dem_votes"]
        .groupby("candidate", as_index=False)["votes_num"]
        .sum()
        .sort_values("votes_num", ascending=False)
    )
    rep_c = (
        df[df["party_group"] == "rep_votes"]
        .groupby("candidate", as_index=False)["votes_num"]
        .sum()
        .sort_values("votes_num", ascending=False)
    )
    dem_candidate = str(dem_c["candidate"].iloc[0]) if not dem_c.empty else ""
    rep_candidate = str(rep_c["candidate"].iloc[0]) if not rep_c.empty else ""
    if infer_office_key(office) == PRESIDENT_OFFICE_KEY:
        dem_candidate = normalize_presidential_candidate_name(dem_candidate)
        rep_candidate = normalize_presidential_candidate_name(rep_candidate)

    # Normalize precinct IDs before allocation/matching.
    df["county"] = df["county"].astype(str).str.strip().str.upper()
    df["precinct"] = df["precinct"].astype(str).str.strip().str.upper()
    df["precinct_id"] = df["county"] + " - " + df["precinct"]
    df = apply_precinct_overrides(df, precinct_overrides)
    allocated = allocate_non_geo_by_candidate(df, precinct_overrides=precinct_overrides)
    # Attach party via candidate+office+county lookup (candidate names are unique enough per office).
    party_lookup = (
        df[["candidate", "party_group"]]
        .drop_duplicates(subset=["candidate"], keep="first")
        .set_index("candidate")["party_group"]
        .to_dict()
    )
    allocated["party_group"] = allocated["candidate"].map(lambda c: party_lookup.get(c, "other_votes"))
    p = allocated.groupby(["precinct_id", "party_group"], as_index=False)["votes"].sum()
    wide = p.pivot(index="precinct_id", columns="party_group", values="votes").fillna(0.0).reset_index()
    for col in ["dem_votes", "rep_votes", "other_votes"]:
        if col not in wide.columns:
            wide[col] = 0.0
    return wide[["precinct_id", "dem_votes", "rep_votes", "other_votes"]], dem_candidate, rep_candidate


def build_precinct_party_votes_county_weight_mode(
    src: pd.DataFrame, office: str, precinct_overrides: dict[str, str] | None = None, election_year: int | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    df = src[src["office"] == office].copy()
    if df.empty:
        empty_p = pd.DataFrame(columns=["precinct_id", "dem_votes", "rep_votes", "other_votes"])
        empty_c = pd.DataFrame(columns=["county", "party_group", "votes"])
        return empty_p, empty_c, "", ""

    df["votes_num"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0.0)
    df["party_group"] = df["party"].map(party_group)
    df = apply_candidate_party_overrides(df, election_year=election_year)
    df["county"] = df["county"].astype(str).str.strip().str.upper()
    df["precinct"] = df["precinct"].astype(str).str.strip().str.upper()
    df["precinct_id"] = df["county"] + " - " + df["precinct"]
    df = apply_precinct_overrides(df, precinct_overrides)
    df["non_geo"] = df["precinct"].map(is_non_geographic_precinct)

    dem_c = (
        df[df["party_group"] == "dem_votes"]
        .groupby("candidate", as_index=False)["votes_num"]
        .sum()
        .sort_values("votes_num", ascending=False)
    )
    rep_c = (
        df[df["party_group"] == "rep_votes"]
        .groupby("candidate", as_index=False)["votes_num"]
        .sum()
        .sort_values("votes_num", ascending=False)
    )
    dem_candidate = str(dem_c["candidate"].iloc[0]) if not dem_c.empty else ""
    rep_candidate = str(rep_c["candidate"].iloc[0]) if not rep_c.empty else ""
    if infer_office_key(office) == PRESIDENT_OFFICE_KEY:
        dem_candidate = normalize_presidential_candidate_name(dem_candidate)
        rep_candidate = normalize_presidential_candidate_name(rep_candidate)

    geo = df[~df["non_geo"]].copy()
    non_geo = df[df["non_geo"]].copy()

    p = geo.groupby(["precinct_id", "party_group"], as_index=False)["votes_num"].sum()
    wide = p.pivot(index="precinct_id", columns="party_group", values="votes_num").fillna(0.0).reset_index()
    for col in ["dem_votes", "rep_votes", "other_votes"]:
        if col not in wide.columns:
            wide[col] = 0.0

    county_non_geo = non_geo.groupby(["county", "party_group"], as_index=False)["votes_num"].sum()
    county_non_geo.columns = ["county", "party_group", "votes"]
    return wide[["precinct_id", "dem_votes", "rep_votes", "other_votes"]], county_non_geo, dem_candidate, rep_candidate


def to_results_df(p: pd.DataFrame, col: str) -> pd.DataFrame:
    out = p[["precinct_id", col]].copy()
    out.columns = ["precinct_id", "votes"]
    out["votes"] = out["votes"].map(lambda v: Decimal(str(v)))
    return out


def agg_party_to_scope(
    precinct_party: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    vap_df: pd.DataFrame,
    map_path: Path,
    block_col: str,
    district_col: str,
    county_shares: pd.DataFrame,
    precinct_bucket_shares: pd.DataFrame,
    matched_precincts: set[str],
    county_non_geo_party: pd.DataFrame | None = None,
) -> tuple[dict[str, int], dict[str, int], dict[str, int], int, int]:
    def _alloc_all_votes_by_bucket_then_county(res_df: pd.DataFrame) -> dict[str, int]:
        """
        Fallback when zero precinct keys match the block->precinct crosswalk.

        Allocates votes to districts using:
        1) county+bucket -> district shares (bucket derived from precinct code like '01-07A' => '01')
        2) remaining county totals -> district shares (county-wide VAP shares)

        This preserves within-county variation at the "precinct bucket" level without requiring
        any matched precinct IDs.
        """
        r = res_df.copy()
        r["precinct_id"] = r["precinct_id"].astype(str).str.strip().str.upper()
        r["votes"] = pd.to_numeric(r["votes"], errors="coerce").fillna(0.0)
        r["county"] = r["precinct_id"].str.split(" - ").str[0].str.strip().str.upper()
        r["precinct"] = r["precinct_id"].str.split(" - ").str[1].fillna("").str.strip().str.upper()
        r["bucket"] = r["precinct"].str.split("-").str[0].str.strip()
        r = r[(r["county"] != "") & (r["votes"] != 0)].copy()
        if r.empty:
            return {}

        add_frames = []
        assigned = pd.DataFrame(columns=["county", "bucket"])

        # Bucket allocation where we have shares.
        u_bucket = r.groupby(["county", "bucket"], as_index=False)["votes"].sum().rename(columns={"votes": "unmatched_votes"})
        b_alloc = u_bucket.merge(precinct_bucket_shares, on=["county", "bucket"], how="inner")
        if not b_alloc.empty:
            b_alloc["alloc_votes"] = b_alloc["unmatched_votes"] * b_alloc["share"]
            add_frames.append(b_alloc[["district", "alloc_votes"]])
            assigned = b_alloc[["county", "bucket"]].drop_duplicates()

        # Remaining county totals (buckets with no shares) fall back to county-wide shares.
        rem = u_bucket
        if not assigned.empty:
            rem = u_bucket.merge(assigned, on=["county", "bucket"], how="left", indicator=True)
            rem = rem[rem["_merge"] == "left_only"].drop(columns=["_merge"])
        if not rem.empty:
            u = rem.groupby("county", as_index=False)["unmatched_votes"].sum().rename(columns={"unmatched_votes": "votes"})
            alloc = u.merge(county_shares, on="county", how="left").dropna(subset=["district", "share"]).copy()
            alloc["alloc_votes"] = alloc["votes"] * alloc["share"]
            add_frames.append(alloc[["district", "alloc_votes"]])

        if not add_frames:
            return {}

        add = pd.concat(add_frames, ignore_index=True).groupby("district", as_index=False)["alloc_votes"].sum()
        return {str(row["district"]).strip(): int(round(float(row["alloc_votes"]))) for _, row in add.iterrows()}

    party_district = {}
    matched = 0
    total = int(len(precinct_party))

    # If nothing matches the precinct crosswalk, skip VAP-shatter and do a pure share-based allocation.
    precinct_ids = precinct_party["precinct_id"].astype(str).str.strip().str.upper()
    if int(precinct_ids.isin(matched_precincts).sum()) == 0:
        for col in ["dem_votes", "rep_votes", "other_votes"]:
            res_df = to_results_df(precinct_party, col)
            party_district[col] = _alloc_all_votes_by_bucket_then_county(res_df)
        return (
            party_district.get("dem_votes", {}),
            party_district.get("rep_votes", {}),
            party_district.get("other_votes", {}),
            0,
            total,
        )

    for col in ["dem_votes", "rep_votes", "other_votes"]:
        res_df = to_results_df(precinct_party, col)
        shattered, audit = shatter_votes(
            results_df=res_df,
            crosswalk_df=crosswalk_df,
            vap_df=vap_df,
            precision=28,
        )
        matched = max(matched, int(len(audit)))
        agg = aggregate_to_districts(shattered, map_path, block_col, district_col)
        party_district[col] = apply_unmatched_county_fallback(
            district_df=agg,
            results_df=res_df,
            matched_precincts=matched_precincts,
            county_shares=county_shares,
            precinct_bucket_shares=precinct_bucket_shares,
        )

        if county_non_geo_party is not None and not county_non_geo_party.empty:
            add_src = county_non_geo_party[county_non_geo_party["party_group"] == col][["county", "votes"]].copy()
            if not add_src.empty:
                add = add_src.merge(county_shares, on="county", how="left").dropna(subset=["district", "share"]).copy()
                add["alloc_votes"] = pd.to_numeric(add["votes"], errors="coerce").fillna(0.0) * pd.to_numeric(
                    add["share"], errors="coerce"
                ).fillna(0.0)
                add = add.groupby("district", as_index=False)["alloc_votes"].sum()
                base = {k: float(v) for k, v in party_district[col].items()}
                for _, row in add.iterrows():
                    d = str(row["district"]).strip()
                    base[d] = float(base.get(d, 0.0)) + float(row["alloc_votes"])
                party_district[col] = {str(k): int(round(v)) for k, v in base.items()}
    return (
        party_district["dem_votes"],
        party_district["rep_votes"],
        party_district["other_votes"],
        matched,
        total,
    )


def build_payload(
    *,
    year: int,
    scope: str,
    contest_type: str,
    office_label: str,
    dem_map: dict[str, int],
    rep_map: dict[str, int],
    oth_map: dict[str, int],
    dem_candidate: str,
    rep_candidate: str,
    matched: int,
    total: int,
) -> dict:
    keys = sorted(set(dem_map) | set(rep_map) | set(oth_map), key=lambda x: (int(x) if str(x).isdigit() else x))
    results = {}
    for k in keys:
        dem = int(dem_map.get(k, 0))
        rep = int(rep_map.get(k, 0))
        oth = int(oth_map.get(k, 0))
        total_votes = dem + rep + oth
        margin = rep - dem
        margin_pct = (margin / total_votes * 100.0) if total_votes else 0.0
        winner = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        results[str(k)] = {
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": oth,
            "total_votes": total_votes,
            "dem_candidate": dem_candidate,
            "rep_candidate": rep_candidate,
            "margin": margin,
            "margin_pct": round(margin_pct, 2),
            "winner": winner,
            "competitiveness": {"color": calculate_competitiveness(margin_pct)},
        }
    cov = (matched / total * 100.0) if total else 0.0
    return {
        "year": year,
        "scope": scope,
        "contest_type": contest_type,
        "meta": {
            "match_coverage_pct": round(cov, 2),
            "matched_precinct_keys": int(matched),
            "total_precinct_keys": int(total),
            "source": "batch_shatter_vap_party_split",
            "office": office_label,
        },
        "general": {"results": results},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build district contest slices with true party split.")
    parser.add_argument("--batch-dir", type=Path, default=Path("data/tmp/shatter/batch_2024_council_judicial_overlay_test"))
    parser.add_argument("--results-csv", type=Path, default=Path("data/2024/20241105__nc__general__precinct.csv"))
    parser.add_argument("--district-contests-dir", type=Path, default=Path("data/district_contests"))
    parser.add_argument("--crosswalk-csv", type=Path, default=Path("data/crosswalks/block20_to_precinct.csv"))
    parser.add_argument("--vap-csv", type=Path, default=Path("data/census/block_vap_2020_nc.csv"))
    parser.add_argument("--house-file", type=Path, default=Path("data/tmp/block_assign_extract/SL 2022-4.csv"))
    parser.add_argument("--senate-file", type=Path, default=Path("data/tmp/block_assign_extract/SL 2022-2.csv"))
    parser.add_argument("--cd-file", type=Path, default=Path("data/census/block files/NC_CD118.txt"))
    parser.add_argument("--allocation-weights-json", type=Path, default=Path("data/mappings/allocation_weights.json"))
    parser.add_argument("--precinct-overrides-csv", type=Path, default=Path("data/mappings/precinct_key_overrides.csv"))
    parser.add_argument(
        "--allocation-year",
        type=int,
        default=None,
        help="Use this year key in allocation_weights.json (defaults to --year).",
    )
    parser.add_argument(
        "--min-county-share",
        type=float,
        default=0.01,
        help="Drop override shares below this threshold and renormalize (e.g., 0.01 => 1%% sliver fallback).",
    )
    parser.add_argument(
        "--nongeo-allocation-mode",
        choices=["precinct_candidate", "county_weights"],
        default="precinct_candidate",
        help="Allocate non-geographic votes by precinct candidate shares (default) or county->district weights.",
    )
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument(
        "--office-source",
        choices=["summary", "auto"],
        default="summary",
        help="Use batch summary office->key mapping, or infer from results CSV using KNOWN_OFFICE_KEYS.",
    )
    args = parser.parse_args()

    src = pd.read_csv(args.results_csv, dtype=str, low_memory=False)
    alloc_year = int(args.allocation_year) if args.allocation_year is not None else int(args.year)
    allocation_weights = load_allocation_weights(args.allocation_weights_json)
    crosswalk_df = load_crosswalk(args.crosswalk_csv, "precinct_id", "block_geoid20")
    vap_df = load_vap(args.vap_csv, "block_geoid20", "vap_count")
    matched_precincts = set(crosswalk_df["precinct_id"].astype(str).str.strip().str.upper().unique())
    src_precinct_ids = (
        src["county"].astype(str).str.strip().str.upper()
        + " - "
        + src["precinct"].astype(str).str.strip().str.upper()
    )
    auto_overrides = build_auto_precinct_overrides(src_precinct_ids, matched_precincts)
    manual_overrides = load_precinct_overrides(args.precinct_overrides_csv, args.year)
    precinct_overrides = {**auto_overrides, **manual_overrides}

    house_map = load_district_map(args.house_file, "Block", "District")
    senate_map = load_district_map(args.senate_file, "Block", "District")
    cd_map = load_district_map(args.cd_file, "GEOID", "CDFP")
    house_shares = apply_county_share_overrides(
        build_county_shares(crosswalk_df, vap_df, house_map),
        year=alloc_year,
        scope="state_house",
        allocation_weights=allocation_weights,
        min_county_share=args.min_county_share,
    )
    house_bucket_shares = build_precinct_bucket_shares(crosswalk_df, vap_df, house_map)
    senate_shares = apply_county_share_overrides(
        build_county_shares(crosswalk_df, vap_df, senate_map),
        year=alloc_year,
        scope="state_senate",
        allocation_weights=allocation_weights,
        min_county_share=args.min_county_share,
    )
    senate_bucket_shares = build_precinct_bucket_shares(crosswalk_df, vap_df, senate_map)
    cd_shares = apply_county_share_overrides(
        build_county_shares(crosswalk_df, vap_df, cd_map),
        year=alloc_year,
        scope="congressional",
        allocation_weights=allocation_weights,
        min_county_share=args.min_county_share,
    )
    cd_bucket_shares = build_precinct_bucket_shares(crosswalk_df, vap_df, cd_map)

    out_dir = args.district_contests_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    offices_to_run: list[tuple[str, str]] = []
    if args.office_source == "summary":
        batch_summary = pd.read_csv(args.batch_dir / "summary.csv", dtype=str).fillna("")
        for _, row in batch_summary.iterrows():
            office = str(row["office"]).strip()
            contest_type = str(row["office_key"]).strip()
            if office and contest_type:
                offices_to_run.append((office, contest_type))
    else:
        seen = set()
        for office in sorted(src["office"].dropna().astype(str).unique()):
            key = infer_office_key(office)
            if key and key not in seen:
                offices_to_run.append((office.strip(), key))
                seen.add(key)

    for office, contest_type in offices_to_run:
        if not office or not contest_type:
            continue
        print(f"Processing {office} -> {contest_type}")
        if args.nongeo_allocation_mode == "county_weights":
            precinct_party, county_non_geo_party, dem_candidate, rep_candidate = build_precinct_party_votes_county_weight_mode(
                src, office, precinct_overrides=precinct_overrides, election_year=args.year
            )
        else:
            precinct_party, dem_candidate, rep_candidate = build_precinct_party_votes(
                src, office, precinct_overrides=precinct_overrides, election_year=args.year
            )
            county_non_geo_party = None
        if precinct_party.empty:
            continue

        dem_h, rep_h, oth_h, matched, total = agg_party_to_scope(
            precinct_party,
            crosswalk_df,
            vap_df,
            args.house_file,
            "Block",
            "District",
            house_shares,
            house_bucket_shares,
            matched_precincts,
            county_non_geo_party=county_non_geo_party,
        )
        dem_s, rep_s, oth_s, _, _ = agg_party_to_scope(
            precinct_party,
            crosswalk_df,
            vap_df,
            args.senate_file,
            "Block",
            "District",
            senate_shares,
            senate_bucket_shares,
            matched_precincts,
            county_non_geo_party=county_non_geo_party,
        )
        dem_c, rep_c, oth_c, _, _ = agg_party_to_scope(
            precinct_party,
            crosswalk_df,
            vap_df,
            args.cd_file,
            "GEOID",
            "CDFP",
            cd_shares,
            cd_bucket_shares,
            matched_precincts,
            county_non_geo_party=county_non_geo_party,
        )

        payloads = {
            f"state_house_{contest_type}_{args.year}.json": build_payload(
                year=args.year,
                scope="state_house",
                contest_type=contest_type,
                office_label=office,
                dem_map=dem_h,
                rep_map=rep_h,
                oth_map=oth_h,
                dem_candidate=dem_candidate,
                rep_candidate=rep_candidate,
                matched=matched,
                total=total,
            ),
            f"state_senate_{contest_type}_{args.year}.json": build_payload(
                year=args.year,
                scope="state_senate",
                contest_type=contest_type,
                office_label=office,
                dem_map=dem_s,
                rep_map=rep_s,
                oth_map=oth_s,
                dem_candidate=dem_candidate,
                rep_candidate=rep_candidate,
                matched=matched,
                total=total,
            ),
            f"congressional_{contest_type}_{args.year}.json": build_payload(
                year=args.year,
                scope="congressional",
                contest_type=contest_type,
                office_label=office,
                dem_map=dem_c,
                rep_map=rep_c,
                oth_map=oth_c,
                dem_candidate=dem_candidate,
                rep_candidate=rep_candidate,
                matched=matched,
                total=total,
            ),
        }
        for name, payload in payloads.items():
            (out_dir / name).write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
            written += 1

    # Rebuild manifest
    manifest = []
    for p in sorted(out_dir.glob("*.json")):
        if p.name == "manifest.json":
            continue
        parts = p.stem.split("_")
        if len(parts) < 3:
            continue
        if parts[0] == "state" and len(parts) >= 4:
            scope = "_".join(parts[0:2])
            contest_type = "_".join(parts[2:-1])
        else:
            scope = parts[0]
            contest_type = "_".join(parts[1:-1])
        try:
            year = int(parts[-1])
        except ValueError:
            continue
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            districts = len(((payload.get("general") or {}).get("results")) or {})
        except Exception:
            districts = 0
        manifest.append(
            {"year": year, "scope": scope, "contest_type": contest_type, "file": p.name, "districts": districts}
        )
    manifest.sort(key=lambda x: (x["year"], x["scope"], x["contest_type"]))
    (out_dir / "manifest.json").write_text(json.dumps({"files": manifest}, indent=2), encoding="utf-8")
    print(f"Wrote {written} slices; manifest updated at {out_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
