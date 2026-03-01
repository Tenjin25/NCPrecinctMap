import argparse
import json
import re
from pathlib import Path

import shapefile  # pyshp
from pyproj import CRS, Transformer
from shapely.geometry import mapping, shape as shapely_shape
from shapely.ops import transform as shapely_transform


def _load_crs_from_prj(prj_path: Path) -> CRS:
    wkt = prj_path.read_text(encoding="utf-8", errors="ignore").strip()
    if not wkt:
        raise ValueError(f"Empty .prj file: {prj_path}")
    return CRS.from_wkt(wkt)


def _reproject_geometry(geom, transformer: Transformer):
    return shapely_transform(lambda x, y, z=None: transformer.transform(x, y), geom)

def _norm_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().upper()


def build_geojson(in_shp: Path, out_geojson: Path, out_centroids: Path | None) -> None:
    prj_path = in_shp.with_suffix(".prj")
    src_crs = _load_crs_from_prj(prj_path)
    dst_crs = CRS.from_epsg(4326)
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)

    reader = shapefile.Reader(str(in_shp))
    field_names = [f[0] for f in reader.fields[1:]]

    features = []
    centroid_features = []

    for shape_record in reader.iterShapeRecords():
        props = dict(zip(field_names, list(shape_record.record)))
        geom = shapely_shape(shape_record.shape.__geo_interface__)
        geom_ll = _reproject_geometry(geom, transformer)

        county = _norm_text(props.get("county_nam", ""))
        prec_id = _norm_text(props.get("prec_id", ""))
        precinct_norm = f"{county} - {prec_id}" if county and prec_id else None
        if county:
            props["county_nam"] = county
        if prec_id:
            props["prec_id"] = prec_id

        feature = {
            "type": "Feature",
            "properties": props,
            "geometry": mapping(geom_ll),
        }
        if precinct_norm:
            feature["properties"]["precinct_norm"] = precinct_norm
        features.append(feature)

        if out_centroids is not None:
            c = geom_ll.representative_point()
            cp = {
                "county_nam": county,
                "prec_id": prec_id,
            }
            if precinct_norm:
                cp["precinct_norm"] = precinct_norm
            centroid_features.append(
                {
                    "type": "Feature",
                    "properties": cp,
                    "geometry": mapping(c),
                }
            )

    fc = {"type": "FeatureCollection", "name": "Voting_Precincts", "features": features}
    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    out_geojson.write_text(json.dumps(fc, separators=(",", ":")), encoding="utf-8")

    if out_centroids is not None:
        cent_fc = {"type": "FeatureCollection", "features": centroid_features}
        out_centroids.parent.mkdir(parents=True, exist_ok=True)
        out_centroids.write_text(json.dumps(cent_fc, separators=(",", ":")), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build data/Voting_Precincts.geojson (EPSG:4326) from an NCSBE precinct shapefile; optionally also write precinct centroids."
    )
    parser.add_argument(
        "--in-shp",
        default="data/census/SBE_PRECINCTS_20240723/SBE_PRECINCTS_20240723.shp",
        help="Input precinct shapefile (.shp)",
    )
    parser.add_argument(
        "--out-geojson",
        default="data/Voting_Precincts.geojson",
        help="Output precinct polygons GeoJSON",
    )
    parser.add_argument(
        "--out-centroids",
        default="data/precinct_centroids.geojson",
        help="Output precinct centroids GeoJSON (set to empty string to skip)",
    )
    args = parser.parse_args()

    in_shp = Path(args.in_shp)
    out_geojson = Path(args.out_geojson)
    out_centroids = Path(args.out_centroids) if str(args.out_centroids).strip() else None

    if not in_shp.exists():
        raise SystemExit(f"Missing input shapefile: {in_shp}")

    build_geojson(in_shp, out_geojson, out_centroids)
    print(f"Wrote {out_geojson}")
    if out_centroids is not None:
        print(f"Wrote {out_centroids}")


if __name__ == "__main__":
    main()
