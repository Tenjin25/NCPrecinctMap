# NCPrecinctMap

Interactive precinct + district election results map for North Carolina, backed by prebuilt JSON slices and (optionally) raw OpenElections precinct CSVs.

This repo focuses on two hard problems:

- **Making historical precinct-level results usable with modern geometry** (precinct IDs change over time; early-vote/absentee buckets don’t map to geography).
- **Showing district results on a single, consistent set of district lines** (reallocated via block/VAP crosswalks where available).

## Features

- **Views**: Counties, Precincts (zoomed in), Congressional Districts, State House, State Senate.
- **Contest picker** driven by manifests (only contests valid for the current view show up).
- **Hover + sidebar details** with margins, vote shares, flip/shift modes, and trend lines.
- **Judicial contests** supported in Counties view when `data/contests/nc_*_<year>.json` slices exist.

## Run locally

`index.html` uses `fetch()` for local files, so you must serve the folder (opening the file directly will usually fail with CORS/file restrictions).

PowerShell:

```powershell
py -m http.server 8000
```

Then open:

- `http://localhost:8000/index.html`

## Data layout (important)

### 1) County/precinct contest slices (used by **Counties** view)

- `data/contests/<contest_type>_<year>.json`
- `data/contests/manifest.json`

These files contain **precinct-level rows** keyed like `"COUNTY - PRECINCT"` and include candidate names:

```json
{ "county": "WAKE - 01-07", "dem_votes": 123, "rep_votes": 456, "dem_candidate": "…", "rep_candidate": "…" }
```

The Counties view aggregates these rows to county totals and also uses them to power precinct hovers (where precinct geometry exists).

### 0) Precinct geometry (used by **Precincts** overlay)

- `data/Voting_Precincts.geojson` (polygons)
- `data/precinct_centroids.geojson` (points; used for high-zoom fallback + indexing)

These are currently built from the NCSBE precinct shapefile in `data/census/SBE_PRECINCTS_20240723/` and reprojected to `EPSG:4326` for web maps:

```powershell
py scripts/build_voting_precincts_geojson.py
```

### 2) District contest slices (used by **District** views)

- `data/district_contests/<scope>_<contest_type>_<year>.json`
- `data/district_contests/manifest.json`

Where:

- `scope ∈ { congressional, state_house, state_senate }`

These files contain already-aggregated district results (plus coverage metadata).

### 3) Base statewide county results (legacy/compat)

- `data/nc_elections_aggregated.json`

This is used as a fallback for some statewide contests/years, but the project increasingly prefers the contest-slice manifests above.

## Precinct matching + “non-geographic” votes

Many precinct exports include buckets like:

- Absentee by mail, One Stop / Early vote, Provisional, Transfer, etc.

Those **do not map to precinct geometry**, and treating them as real precincts will distort maps (especially in Wake/Meck).

The district-building pipeline and the front-end both treat these as **non-geographic** and either:

- keep them only in statewide/county totals, or
- allocate them using candidate shares / county weights (depending on mode).

## Rebuilding district slices (recommended workflow)

The main builder is:

- `scripts/build_district_contests_from_batch_shatter.py`

It reads an OpenElections-style precinct CSV for a given year and produces:

- 3 district slice files (congressional/state_house/state_senate) for each contest it processes
- an updated `data/district_contests/manifest.json`

Example: rebuild president + US senate for 2008:

```powershell
py scripts/build_district_contests_from_batch_shatter.py `
  --year 2008 `
  --results-csv data/2008/20081104__nc__general__precinct.csv `
  --office-source auto `
  --contest-type-regex "^(president|us_senate)$"
```

### Improving Wake/Meck pre-2010 allocations

Older years have many precinct keys that don’t match the modern block→precinct crosswalk. When that happens, the builder uses an **unmatched-vote fallback**.

As of March 2026, the fallback buckets coded precincts at the **`##-##` level** (e.g. `01-07A → 01-07`) instead of the overly-coarse `01` bucket. This reduces “vote smearing” in counties like **Wake** and **Mecklenburg**.

If you still see obvious issues:

1. Check `data/reports/unmatched_precinct_examples.csv` for the exact unmatched precinct keys.
2. Add targeted overrides in `data/mappings/precinct_key_overrides.csv` (preferred over ad-hoc front-end hacks).
3. Rebuild the affected year(s).

## Adding contests to the Counties dropdown

The Counties view only knows about contests that exist in `data/contests/manifest.json`.

If a contest exists in `data/district_contests/*` but not in `data/contests/*`, it will work in district views but **won’t load in Counties**.

You can write county/precinct contest slices from the same builder by enabling `--write-contests` (or use `--contests-only` to skip district aggregation):

```powershell
py scripts/build_district_contests_from_batch_shatter.py `
  --year 2020 `
  --results-csv data/2020/20201103__nc__general__precinct.csv `
  --office-source auto `
  --contest-type-regex "^nc_" `
  --contests-only `
  --write-contests
```

## Troubleshooting

- **“Nothing loads” / console shows fetch errors**: you opened the file directly. Run a local server (`py -m http.server`).
- **Contest shows but hover displays just `D`/`R`**: candidate names are missing in that slice. District hover now falls back to `data/contests/<contest>_<year>.json` when available.
- **New contests don’t show in dropdown**: ensure the correct manifest is updated:
  - Counties view → `data/contests/manifest.json`
  - District views → `data/district_contests/manifest.json`
- **Wake/Meck district accuracy looks off in older years**: check unmatched precinct reports and add overrides; rebuild slices.

## District descriptions (optional)

If you want labels like “Sampson & Bladen Counties” or “Concord–Harrisburg” to appear in district hovers/sidebars, add them to:

- `data/district_descriptions.json`

Format:

```json
{
  "congressional": { "13": "Wake County (Raleigh) + Johnston (partial)" },
  "state_house": { "037": "Cary + Apex (West Wake)" },
  "state_senate": { "019": "Sampson & Bladen Counties" }
}
```

## Notes / disclaimer

- This is a personal/data engineering project. Treat results as **best-effort** until validated against official canvass totals.
- Precinct and district boundary vintages vary by year; reallocation is an approximation that depends on crosswalk coverage.
