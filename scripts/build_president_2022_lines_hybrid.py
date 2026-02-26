#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

import build_district_contests_from_batch_shatter as bdc
import aggregate_dra_block_to_district as draagg


def find_results_csv(data_root: Path, year: int) -> Path:
    ydir = data_root / str(year)
    cands = sorted(ydir.glob("*__general__precinct.csv"))
    if not cands:
        raise FileNotFoundError(f"No general precinct CSV found in {ydir}")
    # Use latest file in lexical/date order.
    return cands[-1]


def detect_president_office(src: pd.DataFrame) -> str:
    vals = sorted(set(src["office"].dropna().astype(str).str.strip()))
    for v in vals:
        u = v.upper()
        if "PRESIDENT" in u and "REPRESENTATIVE" not in u and "REPRESENTATIVES" not in u:
            return v
    # fallback exact common labels
    for candidate in ["US PRESIDENT", "PRESIDENT", "PRESIDENT-VICE PRESIDENT"]:
        if candidate in [x.upper() for x in vals]:
            return candidate
    raise ValueError("Could not detect president office label in source file.")


def write_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def maps_from_rows(rows: list[dict]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    dem: dict[str, int] = {}
    rep: dict[str, int] = {}
    oth: dict[str, int] = {}
    for row in rows:
        d = str(row["district"]).strip().lstrip("0") or "0"
        dem[d] = int(row["dem_votes"])
        rep[d] = int(row["rep_votes"])
        oth[d] = int(row["other_votes"])
    return dem, rep, oth


def parse_makecsv_log_totals(path: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"^(E_[0-9]{2}_[A-Z0-9]+_(?:Total|Dem|Rep));\s*([0-9]+);\s*([0-9]+);\s*[-0-9]+", line.strip())
        if not m:
            continue
        out[m.group(1)] = int(m.group(3))
    return out


def build_county_party_totals(src: pd.DataFrame, office: str) -> pd.DataFrame:
    df = src[src["office"].astype(str).str.strip() == office].copy()
    if df.empty:
        return pd.DataFrame(columns=["county", "dem_votes", "rep_votes", "other_votes"])
    df["county"] = df["county"].astype(str).str.strip().str.upper()
    df["votes_num"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0.0)
    df["party_group"] = df["party"].map(bdc.party_group)
    g = df.groupby(["county", "party_group"], as_index=False)["votes_num"].sum()
    w = g.pivot(index="county", columns="party_group", values="votes_num").fillna(0.0).reset_index()
    for col in ["dem_votes", "rep_votes", "other_votes"]:
        if col not in w.columns:
            w[col] = 0.0
    return w[["county", "dem_votes", "rep_votes", "other_votes"]]


def allocate_county_to_district(county_totals: pd.DataFrame, county_shares: pd.DataFrame) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    if county_totals.empty:
        return {}, {}, {}
    m = county_totals.merge(county_shares, on="county", how="inner")
    if m.empty:
        return {}, {}, {}
    m["dem_alloc"] = pd.to_numeric(m["dem_votes"], errors="coerce").fillna(0.0) * pd.to_numeric(m["share"], errors="coerce").fillna(0.0)
    m["rep_alloc"] = pd.to_numeric(m["rep_votes"], errors="coerce").fillna(0.0) * pd.to_numeric(m["share"], errors="coerce").fillna(0.0)
    m["oth_alloc"] = pd.to_numeric(m["other_votes"], errors="coerce").fillna(0.0) * pd.to_numeric(m["share"], errors="coerce").fillna(0.0)
    g = m.groupby("district", as_index=False)[["dem_alloc", "rep_alloc", "oth_alloc"]].sum()
    dem = {str(r["district"]).strip(): int(round(float(r["dem_alloc"]))) for _, r in g.iterrows()}
    rep = {str(r["district"]).strip(): int(round(float(r["rep_alloc"]))) for _, r in g.iterrows()}
    oth = {str(r["district"]).strip(): int(round(float(r["oth_alloc"]))) for _, r in g.iterrows()}
    return dem, rep, oth


def load_county_name_to_fips(path: Path) -> dict[str, str]:
    g = gpd.read_file(path)[["COUNTYFP20", "NAME20"]].copy()
    g["COUNTYFP20"] = g["COUNTYFP20"].astype(str).str.zfill(3)
    g["NAME20"] = g["NAME20"].astype(str).str.upper().str.strip()
    return dict(zip(g["NAME20"], g["COUNTYFP20"]))


def build_vtd_overlay_shares(vtd_glob: str, district_shp: Path, district_col: str, county_col: str, vtd_col: str) -> pd.DataFrame:
    shp_files = sorted(Path().glob(vtd_glob))
    if not shp_files:
        return pd.DataFrame(columns=["countyfp", "vtdst", "district", "share"])
    parts = []
    for shp in shp_files:
        g = gpd.read_file(shp)[[county_col, vtd_col, "geometry"]].copy()
        parts.append(g)
    vtd = pd.concat(parts, ignore_index=True)
    vtd = gpd.GeoDataFrame(vtd, geometry="geometry", crs=parts[0].crs)
    vtd["countyfp"] = vtd[county_col].astype(str).str.zfill(3)
    vtd["vtdst"] = vtd[vtd_col].astype(str).str.upper().str.strip()
    vtd = vtd[["countyfp", "vtdst", "geometry"]].dropna().copy()

    dist = gpd.read_file(district_shp)[[district_col, "geometry"]].copy()
    dist["district"] = dist[district_col].astype(str).str.strip().str.lstrip("0")
    dist.loc[dist["district"] == "", "district"] = "0"
    dist = dist[["district", "geometry"]]

    vtd_m = vtd.to_crs(3857)
    dist_m = dist.to_crs(3857)
    vtd_m["vtd_area"] = vtd_m.geometry.area
    inter = gpd.overlay(vtd_m, dist_m, how="intersection")
    if inter.empty:
        return pd.DataFrame(columns=["countyfp", "vtdst", "district", "share"])
    inter["iarea"] = inter.geometry.area
    inter["share"] = inter["iarea"] / inter["vtd_area"]
    out = inter.groupby(["countyfp", "vtdst", "district"], as_index=False)["share"].sum()
    return out


def extract_vtd_code(precinct: str) -> str:
    """
    Extract a stable precinct/VTD token from NCSBE-style precinct strings.

    Wake (and some other counties) often embed a code like "01-14" in strings like
    "PRECINCT 01-14A". For those, we normalize to "01-14" to match BAF/VTD keys.
    """
    p = str(precinct).strip().upper()
    if not p:
        return ""

    # Wake/Cabarrus-style codes embedded anywhere in the string.
    # Also strips suffix letters: "01-14A" -> "01-14".
    m = re.search(r"\b(\d{2}-\d{2})[A-Z]?\b", p)
    if m:
        return m.group(1)

    # Drop boilerplate word that can appear as the first token.
    p2 = p.replace("PRECINCT", " ").strip()
    if not p2:
        return ""

    # If no code is present, fall back to the full normalized label (some VTD layers use names).
    return p2


def build_vtd_party_totals(src: pd.DataFrame, office: str, county_name_to_fips: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = src[src["office"].astype(str).str.strip() == office].copy()
    if df.empty:
        empty = pd.DataFrame(columns=["countyfp", "vtdst", "party_group", "votes"])
        return empty, pd.DataFrame(columns=["countyfp", "party_group", "votes"])

    df["county"] = df["county"].astype(str).str.upper().str.strip()
    df["countyfp"] = df["county"].map(lambda c: county_name_to_fips.get(c, ""))
    df["precinct"] = df["precinct"].astype(str).str.upper().str.strip()
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce").fillna(0.0)
    df["party_group"] = df["party"].map(bdc.party_group)
    df["non_geo"] = df["precinct"].map(bdc.is_non_geographic_precinct)
    df["vtdst"] = df["precinct"].map(extract_vtd_code)

    geo = df[(~df["non_geo"]) & (df["countyfp"] != "") & (df["vtdst"] != "")].copy()
    non_geo = df[df["non_geo"] | (df["vtdst"] == "") | (df["countyfp"] == "")].copy()

    geo_g = geo.groupby(["countyfp", "vtdst", "party_group"], as_index=False)["votes"].sum()
    ng = non_geo.groupby(["countyfp", "party_group"], as_index=False)["votes"].sum()
    ng = ng[ng["countyfp"] != ""].copy()
    return geo_g, ng


def allocate_vtd_from_non_geo(geo_vtd_party: pd.DataFrame, non_geo_county_party: pd.DataFrame) -> pd.DataFrame:
    if geo_vtd_party.empty:
        return geo_vtd_party
    out = geo_vtd_party.copy()
    if non_geo_county_party.empty:
        return out

    # party-specific share within county across VTD00
    den = out.groupby(["countyfp", "party_group"], as_index=False)["votes"].sum().rename(columns={"votes": "den"})
    w = out.merge(den, on=["countyfp", "party_group"], how="left")
    w["share"] = w["votes"] / w["den"].replace(0, pd.NA)
    add = non_geo_county_party.merge(
        w[["countyfp", "vtdst", "party_group", "share"]],
        on=["countyfp", "party_group"],
        how="left",
    )
    add["share"] = pd.to_numeric(add["share"], errors="coerce").fillna(0.0)
    add["votes_add"] = pd.to_numeric(add["votes"], errors="coerce").fillna(0.0) * add["share"]
    add = add.groupby(["countyfp", "vtdst", "party_group"], as_index=False)["votes_add"].sum()

    out = out.merge(add, on=["countyfp", "vtdst", "party_group"], how="left")
    out["votes_add"] = pd.to_numeric(out["votes_add"], errors="coerce").fillna(0.0)
    out["votes"] = pd.to_numeric(out["votes"], errors="coerce").fillna(0.0) + out["votes_add"]
    return out[["countyfp", "vtdst", "party_group", "votes"]]


def load_nhgis_vtd00_vap_2000(path: Path) -> pd.DataFrame:
    """
    Load NHGIS 2000 votedist VAP (18+) for NC.

    Expected columns (nhgis ds145 2000 votedist):
      - STATEA (2-digit)
      - COUNTYA (3-digit)
      - VOTEDISTA (VTD code within county)
      - FJ8001 (total 18+)
    Returns: countyfp, vtdst, vap
    """
    if not path.exists():
        return pd.DataFrame(columns=["countyfp", "vtdst", "vap"])
    usecols = ["STATEA", "COUNTYA", "VOTEDISTA", "FJ8001"]
    df = pd.read_csv(path, dtype=str, usecols=usecols).fillna("")
    df["STATEA"] = df["STATEA"].astype(str).str.strip().str.zfill(2)
    df = df[df["STATEA"] == "37"].copy()
    df["countyfp"] = df["COUNTYA"].astype(str).str.strip().str.zfill(3)
    df["vtdst"] = df["VOTEDISTA"].astype(str).str.upper().str.strip()
    df["vap"] = pd.to_numeric(df["FJ8001"], errors="coerce").fillna(0.0)
    df = df[(df["countyfp"] != "") & (df["vtdst"] != "")].copy()
    g = df.groupby(["countyfp", "vtdst"], as_index=False)["vap"].sum()
    return g


def allocate_vtd_from_non_geo_vap(
    geo_vtd_party: pd.DataFrame,
    non_geo_county_party: pd.DataFrame,
    vtd_vap: pd.DataFrame,
) -> pd.DataFrame:
    """
    Allocate non-geographic county party totals to VTDs using VTD VAP shares.

    This is mainly intended for pre-2008 work where block VAP isn't available.
    Falls back to candidate-performance allocation when VAP is unavailable.
    """
    if non_geo_county_party.empty:
        return geo_vtd_party
    if vtd_vap.empty:
        # No VAP table, use the existing candidate-performance allocator.
        return allocate_vtd_from_non_geo(geo_vtd_party, non_geo_county_party)

    # Build VTD shares within county from VAP.
    vv = vtd_vap.copy()
    vv["countyfp"] = vv["countyfp"].astype(str).str.zfill(3)
    vv["vtdst"] = vv["vtdst"].astype(str).str.upper().str.strip()
    vv["vap"] = pd.to_numeric(vv["vap"], errors="coerce").fillna(0.0)
    vv = vv[(vv["countyfp"] != "") & (vv["vtdst"] != "") & (vv["vap"] >= 0)].copy()
    if vv.empty:
        return allocate_vtd_from_non_geo(geo_vtd_party, non_geo_county_party)
    den = vv.groupby("countyfp", as_index=False)["vap"].sum().rename(columns={"vap": "den"})
    vv = vv.merge(den, on="countyfp", how="left")
    vv["share"] = vv["vap"] / vv["den"].replace(0, pd.NA)
    vv["share"] = pd.to_numeric(vv["share"], errors="coerce").fillna(0.0)
    vv = vv[vv["share"] > 0].copy()
    if vv.empty:
        return allocate_vtd_from_non_geo(geo_vtd_party, non_geo_county_party)

    # Expand county party totals across VTDs by VAP share.
    add = non_geo_county_party.copy()
    add["countyfp"] = add["countyfp"].astype(str).str.zfill(3)
    add["votes"] = pd.to_numeric(add["votes"], errors="coerce").fillna(0.0)
    add = add.merge(vv[["countyfp", "vtdst", "share"]], on="countyfp", how="inner")
    if add.empty:
        return allocate_vtd_from_non_geo(geo_vtd_party, non_geo_county_party)
    add["votes_add"] = add["votes"] * add["share"]
    add = add.groupby(["countyfp", "vtdst", "party_group"], as_index=False)["votes_add"].sum()

    if geo_vtd_party.empty:
        out = add.rename(columns={"votes_add": "votes"}).copy()
        return out[["countyfp", "vtdst", "party_group", "votes"]]

    out = geo_vtd_party.copy()
    out = out.merge(add, on=["countyfp", "vtdst", "party_group"], how="left")
    out["votes_add"] = pd.to_numeric(out["votes_add"], errors="coerce").fillna(0.0)
    out["votes"] = pd.to_numeric(out["votes"], errors="coerce").fillna(0.0) + out["votes_add"]
    return out[["countyfp", "vtdst", "party_group", "votes"]]


def vtd_to_district_maps(
    vtd_party: pd.DataFrame,
    shares: pd.DataFrame,
    county_scales: pd.DataFrame | None = None,
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    if vtd_party.empty or shares.empty:
        return {}, {}, {}
    m = vtd_party.merge(shares, on=["countyfp", "vtdst"], how="inner")
    if m.empty:
        return {}, {}, {}
    if county_scales is not None and not county_scales.empty:
        m = m.merge(county_scales, on=["countyfp", "party_group"], how="left")
        m["scale"] = pd.to_numeric(m["scale"], errors="coerce").fillna(1.0)
        m["votes"] = pd.to_numeric(m["votes"], errors="coerce").fillna(0.0) * m["scale"]
    m["alloc"] = pd.to_numeric(m["votes"], errors="coerce").fillna(0.0) * pd.to_numeric(m["share"], errors="coerce").fillna(0.0)
    p = m.groupby(["district", "party_group"], as_index=False)["alloc"].sum()
    w = p.pivot(index="district", columns="party_group", values="alloc").fillna(0.0).reset_index()
    for col in ["dem_votes", "rep_votes", "other_votes"]:
        if col not in w.columns:
            w[col] = 0.0
    dem = {str(r["district"]).strip().lstrip("0") or "0": int(round(float(r["dem_votes"]))) for _, r in w.iterrows()}
    rep = {str(r["district"]).strip().lstrip("0") or "0": int(round(float(r["rep_votes"]))) for _, r in w.iterrows()}
    oth = {str(r["district"]).strip().lstrip("0") or "0": int(round(float(r["other_votes"]))) for _, r in w.iterrows()}
    return dem, rep, oth


def sum_payload_votes(payload: dict) -> tuple[int, int, int]:
    results = (((payload.get("general") or {}).get("results")) or {})
    dem = sum(int(v.get("dem_votes") or 0) for v in results.values())
    rep = sum(int(v.get("rep_votes") or 0) for v in results.values())
    total = sum(int(v.get("total_votes") or 0) for v in results.values())
    return dem, rep, total


def load_block_county_targets(block_csv: Path, prefix: str) -> pd.DataFrame:
    if not block_csv.exists():
        return pd.DataFrame(columns=["countyfp", "dem_target", "rep_target", "other_target", "total_target"])
    total_col = f"{prefix}_Total"
    dem_col = f"{prefix}_Dem"
    rep_col = f"{prefix}_Rep"
    header = pd.read_csv(block_csv, nrows=0).columns.tolist()
    if any(c not in header for c in ["GEOID", total_col, dem_col, rep_col]):
        return pd.DataFrame(columns=["countyfp", "dem_target", "rep_target", "other_target", "total_target"])
    usecols = ["GEOID", total_col, dem_col, rep_col]
    out_parts = []
    for chunk in pd.read_csv(block_csv, dtype=str, usecols=usecols, chunksize=400_000):
        chunk["GEOID"] = chunk["GEOID"].astype(str).str.strip().str.zfill(15)
        chunk["countyfp"] = chunk["GEOID"].str[2:5]
        chunk[total_col] = pd.to_numeric(chunk[total_col], errors="coerce").fillna(0.0)
        chunk[dem_col] = pd.to_numeric(chunk[dem_col], errors="coerce").fillna(0.0)
        chunk[rep_col] = pd.to_numeric(chunk[rep_col], errors="coerce").fillna(0.0)
        chunk["other"] = chunk[total_col] - chunk[dem_col] - chunk[rep_col]
        chunk.loc[chunk["other"] < 0, "other"] = 0.0
        g = chunk.groupby("countyfp", as_index=False)[[dem_col, rep_col, "other", total_col]].sum()
        out_parts.append(g)
    if not out_parts:
        return pd.DataFrame(columns=["countyfp", "dem_target", "rep_target", "other_target", "total_target"])
    a = pd.concat(out_parts, ignore_index=True).groupby("countyfp", as_index=False).sum()
    a.columns = ["countyfp", "dem_target", "rep_target", "other_target", "total_target"]
    return a


def county_scales_from_targets(vtd_party: pd.DataFrame, county_targets: pd.DataFrame) -> pd.DataFrame:
    if vtd_party.empty or county_targets.empty:
        return pd.DataFrame(columns=["countyfp", "party_group", "scale"])
    g = vtd_party.groupby(["countyfp", "party_group"], as_index=False)["votes"].sum()
    w = g.pivot(index="countyfp", columns="party_group", values="votes").fillna(0.0).reset_index()
    for col in ["dem_votes", "rep_votes", "other_votes"]:
        if col not in w.columns:
            w[col] = 0.0
    m = w.merge(county_targets, on="countyfp", how="inner")
    rows = []
    for _, r in m.iterrows():
        rows.append({"countyfp": r["countyfp"], "party_group": "dem_votes", "scale": (float(r["dem_target"]) / float(r["dem_votes"])) if float(r["dem_votes"]) > 0 else 1.0})
        rows.append({"countyfp": r["countyfp"], "party_group": "rep_votes", "scale": (float(r["rep_target"]) / float(r["rep_votes"])) if float(r["rep_votes"]) > 0 else 1.0})
        rows.append({"countyfp": r["countyfp"], "party_group": "other_votes", "scale": (float(r["other_target"]) / float(r["other_votes"])) if float(r["other_votes"]) > 0 else 1.0})
    return pd.DataFrame(rows)


def load_nhgis_bridge_2000_to_2020(blk2000_2010_csv: Path, blk2010_2020_csv: Path) -> pd.DataFrame:
    if not blk2000_2010_csv.exists() or not blk2010_2020_csv.exists():
        return pd.DataFrame(columns=["blk2000ge", "blk2020ge", "weight"])

    a = pd.read_csv(blk2000_2010_csv, dtype=str, usecols=["blk2000ge", "blk2010ge", "weight"]).fillna("")
    b = pd.read_csv(blk2010_2020_csv, dtype=str, usecols=["blk2010ge", "blk2020ge", "weight"]).fillna("")

    a["blk2000ge"] = a["blk2000ge"].astype(str).str.strip().str.zfill(15)
    a["blk2010ge"] = a["blk2010ge"].astype(str).str.strip().str.zfill(15)
    b["blk2010ge"] = b["blk2010ge"].astype(str).str.strip().str.zfill(15)
    b["blk2020ge"] = b["blk2020ge"].astype(str).str.strip().str.zfill(15)

    a["w1"] = pd.to_numeric(a["weight"], errors="coerce").fillna(0.0)
    b["w2"] = pd.to_numeric(b["weight"], errors="coerce").fillna(0.0)

    # Keep NC-targeted chain on the 2010/2020 side.
    a = a[a["blk2010ge"].str.startswith("37") & (a["w1"] > 0)].copy()
    b = b[b["blk2010ge"].str.startswith("37") & b["blk2020ge"].str.startswith("37") & (b["w2"] > 0)].copy()
    if a.empty or b.empty:
        return pd.DataFrame(columns=["blk2000ge", "blk2020ge", "weight"])

    m = a[["blk2000ge", "blk2010ge", "w1"]].merge(
        b[["blk2010ge", "blk2020ge", "w2"]], on="blk2010ge", how="inner"
    )
    if m.empty:
        return pd.DataFrame(columns=["blk2000ge", "blk2020ge", "weight"])

    m["weight"] = pd.to_numeric(m["w1"], errors="coerce").fillna(0.0) * pd.to_numeric(m["w2"], errors="coerce").fillna(0.0)
    m = m[m["weight"] > 0].copy()
    if m.empty:
        return pd.DataFrame(columns=["blk2000ge", "blk2020ge", "weight"])

    g = m.groupby(["blk2000ge", "blk2020ge"], as_index=False)["weight"].sum()
    den = g.groupby("blk2000ge", as_index=False)["weight"].sum().rename(columns={"weight": "wden"})
    g = g.merge(den, on="blk2000ge", how="left")
    g["weight"] = g["weight"] / g["wden"].replace(0, pd.NA)
    g["weight"] = pd.to_numeric(g["weight"], errors="coerce").fillna(0.0)
    g = g[g["weight"] > 0].copy()
    return g[["blk2000ge", "blk2020ge", "weight"]]


def aggregate_block_df_to_maps(
    block_df: pd.DataFrame,
    house_lookup: dict[str, str],
    senate_lookup: dict[str, str],
    cd_lookup: dict[str, str],
) -> tuple[dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int]]:
    if block_df.empty:
        return {}, {}, {}, {}, {}, {}

    def one_lookup(lookup: dict[str, str]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
        d = block_df.copy()
        d["district"] = d["blk2020ge"].map(lookup)
        d = d[d["district"].notna()].copy()
        if d.empty:
            return {}, {}, {}
        g = d.groupby("district", as_index=False)[["dem_votes", "rep_votes", "other_votes"]].sum()
        dem = {str(r["district"]).strip().lstrip("0") or "0": int(round(float(r["dem_votes"]))) for _, r in g.iterrows()}
        rep = {str(r["district"]).strip().lstrip("0") or "0": int(round(float(r["rep_votes"]))) for _, r in g.iterrows()}
        oth = {str(r["district"]).strip().lstrip("0") or "0": int(round(float(r["other_votes"]))) for _, r in g.iterrows()}
        return dem, rep, oth

    dem_h, rep_h, oth_h = one_lookup(house_lookup)
    dem_s, rep_s, oth_s = one_lookup(senate_lookup)
    dem_c, rep_c, oth_c = one_lookup(cd_lookup)
    return dem_h, rep_h, oth_h, dem_s, rep_s, oth_s, dem_c, rep_c, oth_c


def try_legacy_block_pres_maps(
    prefix: str,
    legacy_block_csvs: list[Path],
    nhgis_bridge: pd.DataFrame,
    house_lookup: dict[str, str],
    senate_lookup: dict[str, str],
    cd_lookup: dict[str, str],
) -> tuple[str, tuple[dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int], dict[str, int]] | None]:
    for csv_path in legacy_block_csvs:
        if not csv_path.exists():
            continue
        header = pd.read_csv(csv_path, nrows=0).columns.tolist()
        total_col = f"{prefix}_Total"
        dem_col = f"{prefix}_Dem"
        rep_col = f"{prefix}_Rep"
        if any(c not in header for c in ["GEOID", total_col, dem_col, rep_col]):
            continue

        # Probe whether GEOID is already block2020-like.
        probe = pd.read_csv(csv_path, dtype=str, usecols=["GEOID"], nrows=5000).fillna("")
        probe["GEOID"] = probe["GEOID"].astype(str).str.strip().str.zfill(15)
        direct_hit = probe["GEOID"].isin(set(house_lookup.keys())).mean() if not probe.empty else 0.0

        if direct_hit >= 0.8:
            house_stats = draagg.aggregate_scope(csv_path, house_lookup, total_col, dem_col, rep_col)
            senate_stats = draagg.aggregate_scope(csv_path, senate_lookup, total_col, dem_col, rep_col)
            cd_stats = draagg.aggregate_scope(csv_path, cd_lookup, total_col, dem_col, rep_col)
            dem_h, rep_h, oth_h = maps_from_rows(draagg.rows_from_stats("state_house", house_stats))
            dem_s, rep_s, oth_s = maps_from_rows(draagg.rows_from_stats("state_senate", senate_stats))
            dem_c, rep_c, oth_c = maps_from_rows(draagg.rows_from_stats("congressional", cd_stats))
            return "legacy_block_direct", (dem_h, rep_h, oth_h, dem_s, rep_s, oth_s, dem_c, rep_c, oth_c)

        if nhgis_bridge.empty:
            continue

        chunks = []
        usecols = ["GEOID", total_col, dem_col, rep_col]
        for chunk in pd.read_csv(csv_path, dtype=str, usecols=usecols, chunksize=400_000):
            chunk["blk2000ge"] = chunk["GEOID"].astype(str).str.strip().str.zfill(15)
            chunk[total_col] = pd.to_numeric(chunk[total_col], errors="coerce").fillna(0.0)
            chunk[dem_col] = pd.to_numeric(chunk[dem_col], errors="coerce").fillna(0.0)
            chunk[rep_col] = pd.to_numeric(chunk[rep_col], errors="coerce").fillna(0.0)
            chunk["other_votes"] = chunk[total_col] - chunk[dem_col] - chunk[rep_col]
            chunk.loc[chunk["other_votes"] < 0, "other_votes"] = 0.0
            m = chunk[["blk2000ge", dem_col, rep_col, "other_votes"]].merge(nhgis_bridge, on="blk2000ge", how="inner")
            if m.empty:
                continue
            m["dem_votes"] = pd.to_numeric(m[dem_col], errors="coerce").fillna(0.0) * pd.to_numeric(m["weight"], errors="coerce").fillna(0.0)
            m["rep_votes"] = pd.to_numeric(m[rep_col], errors="coerce").fillna(0.0) * pd.to_numeric(m["weight"], errors="coerce").fillna(0.0)
            m["other_votes"] = pd.to_numeric(m["other_votes"], errors="coerce").fillna(0.0) * pd.to_numeric(m["weight"], errors="coerce").fillna(0.0)
            g = m.groupby("blk2020ge", as_index=False)[["dem_votes", "rep_votes", "other_votes"]].sum()
            chunks.append(g)

        if not chunks:
            continue
        block2020 = pd.concat(chunks, ignore_index=True).groupby("blk2020ge", as_index=False).sum()
        maps = aggregate_block_df_to_maps(block2020, house_lookup, senate_lookup, cd_lookup)
        if maps[0] or maps[1] or maps[2]:
            return "legacy_block2000_nhgis_bridge", maps

    return "", None


def main() -> None:
    ap = argparse.ArgumentParser(description="Build president district outputs on 2022 lines with hybrid DRA-block/official-shatter mode.")
    ap.add_argument("--years", default="2000,2004,2008,2012,2016,2020,2024")
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--district-contests-dir", type=Path, default=Path("data/district_contests"))
    ap.add_argument("--block-csv", type=Path, default=Path("data/Election_Data_Block_NC.v07/election_data_block_NC.v07.csv"))
    ap.add_argument("--makecsv-log", type=Path, default=Path("data/Election_Data_Block_NC.v07/makecsv_election.log"))
    ap.add_argument("--house-map", type=Path, default=Path("data/crosswalks/block20_to_2022_state_house.csv"))
    ap.add_argument("--senate-map", type=Path, default=Path("data/crosswalks/block20_to_2022_state_senate.csv"))
    ap.add_argument("--cd-map", type=Path, default=Path("data/crosswalks/block20_to_cd118.csv"))
    ap.add_argument("--crosswalk-csv", type=Path, default=Path("data/crosswalks/block20_to_precinct.csv"))
    ap.add_argument("--vap-csv", type=Path, default=Path("data/census/block_vap_2020_nc.csv"))
    ap.add_argument("--allocation-weights-json", type=Path, default=Path("data/mappings/allocation_weights.json"))
    ap.add_argument("--precinct-overrides-csv", type=Path, default=Path("data/mappings/precinct_key_overrides.csv"))
    ap.add_argument("--allocation-year", type=int, default=2022)
    ap.add_argument("--strict", action="store_true", help="Fail when validation checks fail.")
    ap.add_argument(
        "--vtd00-shp-glob",
        default="data/census/tiger2008_vtd00_counties_extracted/tl_2008_*_vtd00/tl_2008_*_vtd00.shp",
    )
    ap.add_argument("--vtd10-shp", type=Path, default=Path("data/census/tl_2012_37_vtd10/tl_2012_37_vtd10.shp"))
    ap.add_argument("--county20-geojson", type=Path, default=Path("data/census/tl_2020_37_county20.geojson"))
    ap.add_argument(
        "--nhgis-blk2000-blk2010-csv",
        type=Path,
        default=Path("data/census/nhgis_blk2000_blk2010_37/nhgis_blk2000_blk2010_37.csv"),
    )
    ap.add_argument(
        "--nhgis-blk2010-blk2020-csv",
        type=Path,
        default=Path("data/census/nhgis_blk2010_blk2020_37/nhgis_blk2010_blk2020_37.csv"),
    )
    ap.add_argument(
        "--nhgis-vtd00-vap-csv",
        type=Path,
        default=Path("data/census/nhgis0004_csv/nhgis0004_csv/nhgis0004_ds145_2000_votedist.csv"),
        help="NHGIS 2000 votedist VAP table (used for pre-2008 VTD proxy allocation when precinct->block shatter can't match).",
    )
    ap.add_argument(
        "--pre2008-vtd-nongeo-mode",
        choices=["candidate_perf", "vtd_vap"],
        default="candidate_perf",
        help="When using the VTD overlay proxy (pre-2008), how to distribute county non-geographic buckets to VTDs.",
    )
    ap.add_argument(
        "--county-calibration-block-csvs",
        default="data/Election_Data_Block_NC.v07/Election_Data_Block_NC.v02/election_data_block_NC.v02.csv,data/Election_Data_Block_NC.v07/Election_Data_Block_NC.v01/election_data_block_NC.v01.csv,data/Election_Data_Block_NC.v07/election_data_block_NC.v07.csv",
        help="Comma-separated block CSVs checked in order for E_YY_PRES county totals used to calibrate proxy years.",
    )
    ap.add_argument(
        "--include-statewide",
        action="store_true",
        help="Also build Council of State + statewide judicial + US Senate slices via build_district_contests_from_batch_shatter.py before re-writing president slices.",
    )
    ap.add_argument(
        "--statewide-min-year",
        type=int,
        default=2016,
        help="When --include-statewide is set, only run statewide builder for years >= this value.",
    )
    args = ap.parse_args()

    years = [int(x.strip()) for x in args.years.split(",") if x.strip()]

    if args.include_statewide:
        statewide_script = Path(__file__).resolve().with_name("build_district_contests_from_batch_shatter.py")
        for year in sorted(set(years)):
            if year < int(args.statewide_min_year):
                continue
            results_csv = find_results_csv(args.data_root, year)
            cmd = [
                sys.executable,
                str(statewide_script),
                "--results-csv",
                str(results_csv),
                "--year",
                str(year),
                "--office-source",
                "auto",
                "--nongeo-allocation-mode",
                "county_weights",
                "--allocation-year",
                str(args.allocation_year),
                "--allocation-weights-json",
                str(args.allocation_weights_json),
                "--precinct-overrides-csv",
                str(args.precinct_overrides_csv),
                "--district-contests-dir",
                str(args.district_contests_dir),
                "--crosswalk-csv",
                str(args.crosswalk_csv),
                "--vap-csv",
                str(args.vap_csv),
            ]
            subprocess.run(cmd, check=True)
            print(f"[{year}] built statewide/judicial/us_senate slices via batch shatter")

    header = pd.read_csv(args.block_csv, nrows=0).columns.tolist() if args.block_csv.exists() else []
    log_totals = parse_makecsv_log_totals(args.makecsv_log)

    # Load shared shatter inputs once.
    crosswalk_df = bdc.load_crosswalk(args.crosswalk_csv, "precinct_id", "block_geoid20")
    vap_df = bdc.load_vap(args.vap_csv, "block_geoid20", "vap_count")
    matched_precincts = set(crosswalk_df["precinct_id"].astype(str).str.strip().str.upper().unique())
    house_map_df = bdc.load_district_map(args.house_map, "block_geoid20", "district")
    senate_map_df = bdc.load_district_map(args.senate_map, "block_geoid20", "district")
    cd_map_df = bdc.load_district_map(args.cd_map, "block_geoid20", "district")
    allocation_weights = bdc.load_allocation_weights(args.allocation_weights_json)
    house_shares = bdc.apply_county_share_overrides(
        bdc.build_county_shares(crosswalk_df, vap_df, house_map_df),
        year=args.allocation_year,
        scope="state_house",
        allocation_weights=allocation_weights,
        min_county_share=0.01,
    )
    senate_shares = bdc.apply_county_share_overrides(
        bdc.build_county_shares(crosswalk_df, vap_df, senate_map_df),
        year=args.allocation_year,
        scope="state_senate",
        allocation_weights=allocation_weights,
        min_county_share=0.01,
    )
    cd_shares = bdc.apply_county_share_overrides(
        bdc.build_county_shares(crosswalk_df, vap_df, cd_map_df),
        year=args.allocation_year,
        scope="congressional",
        allocation_weights=allocation_weights,
        min_county_share=0.01,
    )
    house_bucket = bdc.build_precinct_bucket_shares(crosswalk_df, vap_df, house_map_df)
    senate_bucket = bdc.build_precinct_bucket_shares(crosswalk_df, vap_df, senate_map_df)
    cd_bucket = bdc.build_precinct_bucket_shares(crosswalk_df, vap_df, cd_map_df)

    # Pre-2008 VTD00 spatial overlay assets.
    county_name_to_fips = load_county_name_to_fips(args.county20_geojson) if args.county20_geojson.exists() else {}
    vtd10_house = build_vtd_overlay_shares(
        str(args.vtd10_shp),
        Path("data/tl_2022_37_sldl/tl_2022_37_sldl.shp"),
        "SLDLST",
        "COUNTYFP10",
        "VTDST10",
    ) if args.vtd10_shp.exists() else pd.DataFrame(columns=["countyfp", "vtdst", "district", "share"])
    vtd10_senate = build_vtd_overlay_shares(
        str(args.vtd10_shp),
        Path("data/tl_2022_37_sldu/tl_2022_37_sldu.shp"),
        "SLDUST",
        "COUNTYFP10",
        "VTDST10",
    ) if args.vtd10_shp.exists() else pd.DataFrame(columns=["countyfp", "vtdst", "district", "share"])
    vtd10_cd = build_vtd_overlay_shares(
        str(args.vtd10_shp),
        Path("data/tl_2022_37_cd118/tl_2022_37_cd118.shp"),
        "CD118FP",
        "COUNTYFP10",
        "VTDST10",
    ) if args.vtd10_shp.exists() else pd.DataFrame(columns=["countyfp", "vtdst", "district", "share"])

    vtd00_house = build_vtd_overlay_shares(
        args.vtd00_shp_glob,
        Path("data/tl_2022_37_sldl/tl_2022_37_sldl.shp"),
        "SLDLST",
        "COUNTYFP00",
        "VTDST00",
    )
    vtd00_senate = build_vtd_overlay_shares(
        args.vtd00_shp_glob,
        Path("data/tl_2022_37_sldu/tl_2022_37_sldu.shp"),
        "SLDUST",
        "COUNTYFP00",
        "VTDST00",
    )
    vtd00_cd = build_vtd_overlay_shares(
        args.vtd00_shp_glob,
        Path("data/tl_2022_37_cd118/tl_2022_37_cd118.shp"),
        "CD118FP",
        "COUNTYFP00",
        "VTDST00",
    )
    nhgis_bridge_2000_2020 = load_nhgis_bridge_2000_to_2020(
        args.nhgis_blk2000_blk2010_csv, args.nhgis_blk2010_blk2020_csv
    )
    vtd00_vap_2000 = load_nhgis_vtd00_vap_2000(args.nhgis_vtd00_vap_csv)

    # DRA lookups once for speed.
    dra_house_lookup = draagg.build_lookup(draagg.load_map(args.house_map))
    dra_senate_lookup = draagg.build_lookup(draagg.load_map(args.senate_map))
    dra_cd_lookup = draagg.build_lookup(draagg.load_map(args.cd_map))

    summary_rows: list[dict] = []
    failures: list[str] = []
    calib_paths = [Path(x.strip()) for x in str(args.county_calibration_block_csvs).split(",") if x.strip()]

    for year in years:
        results_csv = find_results_csv(args.data_root, year)
        src = pd.read_csv(results_csv, dtype=str, low_memory=False).fillna("")
        if "votes" in src.columns:
            src["votes"] = pd.to_numeric(src["votes"], errors="coerce").fillna(0.0)

        try:
            office = detect_president_office(src)
        except ValueError:
            print(f"[{year}] no presidential office rows found; skipping president slices for this year.")
            continue
        # Candidate names from source file for payload labels.
        src_ids = src["county"].astype(str).str.strip().str.upper() + " - " + src["precinct"].astype(str).str.strip().str.upper()
        auto_overrides = bdc.build_auto_precinct_overrides(src_ids, matched_precincts)
        manual_overrides = bdc.load_precinct_overrides(args.precinct_overrides_csv, year)
        precinct_overrides = {**auto_overrides, **manual_overrides}
        _, dem_candidate, rep_candidate = bdc.build_precinct_party_votes(src, office, precinct_overrides=precinct_overrides)

        prefix = f"E_{year % 100:02d}_PRES"
        has_dra = all(f"{prefix}_{k}" in header for k in ["Total", "Dem", "Rep"])
        county_targets = pd.DataFrame(columns=["countyfp", "dem_target", "rep_target", "other_target", "total_target"])
        county_target_source = ""
        for cp in calib_paths:
            c = load_block_county_targets(cp, prefix)
            if not c.empty:
                county_targets = c
                county_target_source = str(cp)
                break

        if has_dra:
            method = "dra_block_direct"
            house_stats = draagg.aggregate_scope(
                block_csv=args.block_csv,
                lookup=dra_house_lookup,
                total_col=f"{prefix}_Total",
                dem_col=f"{prefix}_Dem",
                rep_col=f"{prefix}_Rep",
            )
            senate_stats = draagg.aggregate_scope(
                block_csv=args.block_csv,
                lookup=dra_senate_lookup,
                total_col=f"{prefix}_Total",
                dem_col=f"{prefix}_Dem",
                rep_col=f"{prefix}_Rep",
            )
            cd_stats = draagg.aggregate_scope(
                block_csv=args.block_csv,
                lookup=dra_cd_lookup,
                total_col=f"{prefix}_Total",
                dem_col=f"{prefix}_Dem",
                rep_col=f"{prefix}_Rep",
            )
            dem_h, rep_h, oth_h = maps_from_rows(draagg.rows_from_stats("state_house", house_stats))
            dem_s, rep_s, oth_s = maps_from_rows(draagg.rows_from_stats("state_senate", senate_stats))
            dem_c, rep_c, oth_c = maps_from_rows(draagg.rows_from_stats("congressional", cd_stats))
            matched = total = 0
        else:
            method = "official_precinct_shatter"
            legacy_method, legacy_maps = try_legacy_block_pres_maps(
                prefix=prefix,
                legacy_block_csvs=calib_paths,
                nhgis_bridge=nhgis_bridge_2000_2020,
                house_lookup=dra_house_lookup,
                senate_lookup=dra_senate_lookup,
                cd_lookup=dra_cd_lookup,
            )
            if legacy_maps is not None:
                (
                    dem_h,
                    rep_h,
                    oth_h,
                    dem_s,
                    rep_s,
                    oth_s,
                    dem_c,
                    rep_c,
                    oth_c,
                ) = legacy_maps
                method = legacy_method
                matched = total = 0
            else:
                precinct_party, dem_candidate, rep_candidate = bdc.build_precinct_party_votes(
                    src, office, precinct_overrides=precinct_overrides
                )
                matched_count = 0 if precinct_party.empty else int(
                    precinct_party["precinct_id"].astype(str).str.strip().str.upper().isin(matched_precincts).sum()
                )
                if precinct_party.empty:
                    print(f"[{year}] skipped (no president rows found).")
                    continue
                if matched_count == 0:
                    # Try VTD00/10 spatial overlay proxy before county-only fallback.
                    geo_vtd_party, non_geo_county_party = build_vtd_party_totals(src, office, county_name_to_fips)
                    if args.pre2008_vtd_nongeo_mode == "vtd_vap" and year <= 2006:
                        vtd_party = allocate_vtd_from_non_geo_vap(geo_vtd_party, non_geo_county_party, vtd00_vap_2000)
                    else:
                        vtd_party = allocate_vtd_from_non_geo(geo_vtd_party, non_geo_county_party)
                    scales = county_scales_from_targets(vtd_party, county_targets)
                    dem_h, rep_h, oth_h = vtd_to_district_maps(vtd_party, vtd10_house, county_scales=scales)
                    dem_s, rep_s, oth_s = vtd_to_district_maps(vtd_party, vtd10_senate, county_scales=scales)
                    dem_c, rep_c, oth_c = vtd_to_district_maps(vtd_party, vtd10_cd, county_scales=scales)
                    if dem_h or rep_h or oth_h:
                        method = "official_vtd10_overlay_proxy_vtd_vap" if args.pre2008_vtd_nongeo_mode == "vtd_vap" and year <= 2006 else "official_vtd10_overlay_proxy"
                        matched = total = 0
                    else:
                        dem_h, rep_h, oth_h = vtd_to_district_maps(vtd_party, vtd00_house, county_scales=scales)
                        dem_s, rep_s, oth_s = vtd_to_district_maps(vtd_party, vtd00_senate, county_scales=scales)
                        dem_c, rep_c, oth_c = vtd_to_district_maps(vtd_party, vtd00_cd, county_scales=scales)
                    if dem_h or rep_h or oth_h:
                        if method == "official_vtd10_overlay_proxy" or method == "official_vtd10_overlay_proxy_vtd_vap":
                            method = method
                        else:
                            method = "official_vtd00_overlay_proxy_vtd_vap" if args.pre2008_vtd_nongeo_mode == "vtd_vap" and year <= 2006 else "official_vtd00_overlay_proxy"
                        matched = total = 0
                    else:
                        method = "official_county_share_proxy"
                        county_totals = build_county_party_totals(src, office)
                        dem_h, rep_h, oth_h = allocate_county_to_district(county_totals, house_shares)
                        dem_s, rep_s, oth_s = allocate_county_to_district(county_totals, senate_shares)
                        dem_c, rep_c, oth_c = allocate_county_to_district(county_totals, cd_shares)
                        matched = total = 0
                else:
                    dem_h, rep_h, oth_h, matched, total = bdc.agg_party_to_scope(
                        precinct_party,
                        crosswalk_df,
                        vap_df,
                        args.house_map,
                        "block_geoid20",
                        "district",
                        house_shares,
                        house_bucket,
                        matched_precincts,
                        county_non_geo_party=None,
                    )
                    dem_s, rep_s, oth_s, _, _ = bdc.agg_party_to_scope(
                        precinct_party,
                        crosswalk_df,
                        vap_df,
                        args.senate_map,
                        "block_geoid20",
                        "district",
                        senate_shares,
                        senate_bucket,
                        matched_precincts,
                        county_non_geo_party=None,
                    )
                    dem_c, rep_c, oth_c, _, _ = bdc.agg_party_to_scope(
                        precinct_party,
                        crosswalk_df,
                        vap_df,
                        args.cd_map,
                        "block_geoid20",
                        "district",
                        cd_shares,
                        cd_bucket,
                        matched_precincts,
                        county_non_geo_party=None,
                    )

        payload_house = bdc.build_payload(
            year=year,
            scope="state_house",
            contest_type="president",
            office_label=office,
            dem_map=dem_h,
            rep_map=rep_h,
            oth_map=oth_h,
            dem_candidate=dem_candidate,
            rep_candidate=rep_candidate,
            matched=matched,
            total=total,
        )
        payload_senate = bdc.build_payload(
            year=year,
            scope="state_senate",
            contest_type="president",
            office_label=office,
            dem_map=dem_s,
            rep_map=rep_s,
            oth_map=oth_s,
            dem_candidate=dem_candidate,
            rep_candidate=rep_candidate,
            matched=matched,
            total=total,
        )
        payload_cd = bdc.build_payload(
            year=year,
            scope="congressional",
            contest_type="president",
            office_label=office,
            dem_map=dem_c,
            rep_map=rep_c,
            oth_map=oth_c,
            dem_candidate=dem_candidate,
            rep_candidate=rep_candidate,
            matched=matched,
            total=total,
        )
        payload_house["meta"]["source"] = method
        payload_senate["meta"]["source"] = method
        payload_cd["meta"]["source"] = method

        write_payload(args.district_contests_dir / f"state_house_president_{year}.json", payload_house)
        write_payload(args.district_contests_dir / f"state_senate_president_{year}.json", payload_senate)
        write_payload(args.district_contests_dir / f"congressional_president_{year}.json", payload_cd)

        dem_sum, rep_sum, total_sum = sum_payload_votes(payload_house)
        expected_dem = log_totals.get(f"{prefix}_Dem")
        expected_rep = log_totals.get(f"{prefix}_Rep")
        expected_total = log_totals.get(f"{prefix}_Total")
        accuracy_pct = ""
        benchmark = ""
        if has_dra and expected_total is not None:
            benchmark = "dra_makecsv_log"
            dem_acc = max(0.0, 100.0 - (abs(dem_sum - expected_dem) / expected_dem * 100.0 if expected_dem else 0.0))
            rep_acc = max(0.0, 100.0 - (abs(rep_sum - expected_rep) / expected_rep * 100.0 if expected_rep else 0.0))
            total_acc = max(0.0, 100.0 - (abs(total_sum - expected_total) / expected_total * 100.0 if expected_total else 0.0))
            accuracy_pct = round((dem_acc + rep_acc + total_acc) / 3.0, 4)
            ok = abs(total_sum - expected_total) <= 2 and abs(dem_sum - expected_dem) <= 2 and abs(rep_sum - expected_rep) <= 2
            if not ok:
                msg = (
                    f"[{year}] validation failed: house sums dem/rep/total="
                    f"{dem_sum}/{rep_sum}/{total_sum} expected {expected_dem}/{expected_rep}/{expected_total}"
                )
                failures.append(msg)
                print(msg)

        r37 = payload_house["general"]["results"].get("37", {})
        r59 = payload_house["general"]["results"].get("59", {})
        summary_rows.append(
            {
                "year": year,
                "method": method,
                "office_label": office,
                "hd37_margin_pct": r37.get("margin_pct", ""),
                "hd59_margin_pct": r59.get("margin_pct", ""),
                "house_dem_total": dem_sum,
                "house_rep_total": rep_sum,
                "house_total_votes": total_sum,
                "accuracy_pct": accuracy_pct,
                "accuracy_benchmark": benchmark,
                "county_calibration_source": county_target_source,
            }
        )
        print(f"[{year}] wrote president slices via {method}")

    summary_path = args.district_contests_dir / "president_hybrid_build_summary.csv"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    print(f"Wrote summary {summary_path}")

    if failures and args.strict:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    main()
