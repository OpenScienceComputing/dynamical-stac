#!/usr/bin/env python3
"""Build a static STAC catalog from all dynamical.org Icechunk stores on AWS Open Data.

Discovers datasets by:
  1. Querying awslabs/open-data-registry for dynamical-*.yaml entries
  2. Listing each public S3 bucket for *.icechunk store prefixes
  3. Opening each store to extract metadata and build STAC items (with full
     cube:dimensions including lead_time, ensemble_member, etc.)
  4. Saving the catalog locally then uploading to R2

Usage:
    # Dry run (no upload):
    python build_catalog.py --no-upload --output-dir /tmp/stac-out

    # Upload to R2:
    python build_catalog.py \\
        --catalog-bucket osc-pub \\
        --catalog-prefix stac/dynamical \\
        --profile osc-pub-r2 \\
        --public-domain r2-pub.openscicomp.io
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import warnings
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import icechunk
import numpy as np
import pystac
import requests
import rioxarray  # noqa: F401 — registers .rio accessor
import s3fs
import xarray as xr
import yaml

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ICECHUNK_MEDIA_TYPE = "application/vnd.zarr+icechunk"
ICECHUNK_STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/storage/v2.0.0/schema.json",
    "https://stac-extensions.github.io/virtual-assets/v1.0.0/schema.json",
    "https://stac-extensions.github.io/zarr/v1.1.0/schema.json",
    "https://stac-extensions.github.io/version/v1.2.0/schema.json",
    "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
]

_REGISTRY_API = (
    "https://api.github.com/repos/awslabs/open-data-registry"
    "/contents/datasets?per_page=300"
)
_REGISTRY_RAW = (
    "https://raw.githubusercontent.com/awslabs/open-data-registry"
    "/main/datasets/{filename}"
)

DYNAMICAL_PROVIDER = pystac.Provider(
    name="dynamical.org",
    roles=["producer", "processor", "host"],
    url="https://dynamical.org",
)


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def make_json_serializable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(v) for v in obj]
    if isinstance(obj, (datetime, np.datetime64)):
        return str(obj)
    try:
        json.dumps(obj)
        return obj
    except (TypeError, OverflowError):
        return str(obj)


def _bbox_to_geometry(bbox: list[float]) -> dict:
    l, b, r, t = bbox
    return {"type": "Polygon", "coordinates": [[[l, b], [l, t], [r, t], [r, b], [l, b]]]}


def extract_spatial_extent_rio(ds: xr.Dataset) -> list[float]:
    """Return [lonmin, latmin, lonmax, latmax] in WGS84 using rioxarray."""
    try:
        first_var = next(iter(ds.data_vars))
        lonmin, latmin, lonmax, latmax = ds[first_var].rio.transform_bounds("EPSG:4326")
        return [
            max(-180.0, lonmin), max(-90.0, latmin),
            min(180.0, lonmax),  min(90.0, latmax),
        ]
    except Exception:
        return [-180.0, -90.0, 180.0, 90.0]


def build_cube_variables(ds: xr.Dataset) -> dict:
    """Return cube:variables dict for all data variables."""
    return {
        dv: {
            "type": "data",
            "dimensions": list(ds[dv].dims),
            "unit": ds[dv].attrs.get("units", "not set"),
            "description": ds[dv].attrs.get("long_name", str(dv)),
        }
        for dv in ds.data_vars
    }


def add_extra_dimensions(item: pystac.Item, ds: xr.Dataset, skip_dims: set[str]) -> None:
    """Add dims absent from cube:dimensions (e.g. lead_time, ensemble_member).

    xstac only populates cube:dimensions for the named temporal/x/y dims.
    This function fills in every remaining dim so that browser clients can
    discover and build selectors for all non-spatial/non-temporal axes.
    """
    cube_dims: dict = item.properties.setdefault("cube:dimensions", {})
    for dim in ds.dims:
        if dim in skip_dims or dim in cube_dims:
            continue
        coord = ds.coords.get(dim)
        if coord is None:
            continue
        entry: dict[str, Any] = {"type": "other"}
        long_name = coord.attrs.get("long_name", dim)
        if long_name != dim:
            entry["description"] = long_name
        if np.issubdtype(coord.dtype, np.timedelta64):
            values_h = (coord.values / np.timedelta64(1, "h")).astype(int).tolist()
            entry["unit"] = "hours"
            entry["extent"] = [int(min(values_h)), int(max(values_h))]
            if len(values_h) > 1:
                steps = np.diff(values_h)
                if np.all(steps == steps[0]):
                    entry["step"] = int(steps[0])
        else:
            values = coord.values.tolist()
            if len(values) <= 100:
                entry["values"] = [make_json_serializable(v) for v in values]
            else:
                entry["extent"] = [
                    make_json_serializable(min(values)),
                    make_json_serializable(max(values)),
                ]
        cube_dims[dim] = entry


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def fetch_registry_entries() -> list[dict]:
    log.info("Querying AWS Open Data Registry for dynamical.org datasets ...")
    token = os.environ.get("GITHUB_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.get(_REGISTRY_API, headers=headers, timeout=30)
    resp.raise_for_status()
    files = [
        f for f in resp.json()
        if f["name"].startswith("dynamical-") and f["name"].endswith(".yaml")
    ]
    log.info("Found %d registry entries", len(files))
    entries = []
    for f in files:
        raw = requests.get(
            _REGISTRY_RAW.format(filename=f["name"]), headers=headers, timeout=30
        )
        raw.raise_for_status()
        entry = yaml.safe_load(raw.text)
        entry["_filename"] = f["name"]
        entries.append(entry)
    return entries


def bucket_from_entry(entry: dict) -> tuple[str, str]:
    for resource in entry.get("Resources", []):
        if resource.get("Type") == "S3 Bucket":
            bucket = resource.get("ARN", "").split(":::")[-1]
            region = resource.get("Region", "us-east-1")
            return bucket, region
    raise ValueError(f"No S3 bucket in registry entry: {entry.get('Name')}")


def discover_icechunk_prefixes(bucket: str, region: str) -> list[str]:
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": region})
    prefixes = []
    try:
        top_paths = fs.ls(bucket, detail=False)
    except Exception as exc:
        log.warning("Cannot list s3://%s: %s", bucket, exc)
        return prefixes
    for top_path in top_paths:
        try:
            sub_paths = fs.ls(top_path, detail=False)
        except Exception:
            continue
        for sub_path in sub_paths:
            if sub_path.split("/")[-1].endswith(".icechunk"):
                prefix = sub_path[len(bucket) + 1:].rstrip("/") + "/"
                prefixes.append(prefix)
                log.info("  Found: s3://%s/%s", bucket, prefix)
    return prefixes


# ---------------------------------------------------------------------------
# Item building
# ---------------------------------------------------------------------------

def build_item_for_store(
    bucket: str,
    prefix: str,
    region: str,
    entry: dict,
) -> pystac.Item | None:
    store_uri = f"s3://{bucket}/{prefix}"
    log.info("Opening %s ...", store_uri)
    try:
        storage = icechunk.s3_storage(
            bucket=bucket, prefix=prefix, region=region, anonymous=True
        )
        repo = icechunk.Repository.open(storage=storage)
        session = repo.readonly_session(branch="main")
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Numcodecs codecs are not in the Zarr version 3 specification.*",
            )
            ds = xr.open_zarr(
                session.store, chunks=None, consolidated=False, zarr_format=3
            )
    except Exception as exc:
        log.warning("  Failed to open %s: %s", store_uri, exc)
        return None

    snap = session.snapshot_id
    log.info("  snapshot: %s  dims: %s", snap, dict(ds.sizes))

    # Stable item ID from store path, e.g. "noaa-gfs-forecast-v0-2-7"
    dataset_name = prefix.split("/")[0]
    version_str  = prefix.split("/")[1]
    version_slug = version_str.replace(".icechunk", "").replace(".", "-")
    item_id = f"{dataset_name}-{version_slug}"

    base_name = entry.get("Name", dataset_name).split("(")[0].strip()
    sub_prod = dataset_name.replace("noaa-", "").replace("nasa-", "").replace("-", " ").title()
    for word in base_name.split():
        sub_prod = sub_prod.replace(word, "").strip()
    clean_version = version_str.replace(".icechunk", "")
    item_title = f"{base_name} ({sub_prod.title()}, {clean_version})"

    # Detect dimensions
    time_dim = next((d for d in ["init_time", "time"] if d in ds.dims), None)
    x_dim    = next((d for d in ["longitude", "lon", "x"] if d in ds.dims), None)
    y_dim    = next((d for d in ["latitude",  "lat", "y"] if d in ds.dims), None)

    ref_sys = 4326
    try:
        if ds.rio.crs:
            ref_sys = ds.rio.crs.to_epsg() or ds.rio.crs.to_wkt()
    except Exception:
        pass

    # Temporal extent
    start_dt = end_dt = None
    if time_dim:
        try:
            start_dt = datetime.fromisoformat(
                str(ds[time_dim].min().values[()]).split(".")[0]
            )
            end_dt = datetime.fromisoformat(
                str(ds[time_dim].max().values[()]).split(".")[0]
            )
        except Exception:
            pass

    bbox = extract_spatial_extent_rio(ds)

    item = pystac.Item(
        id=item_id,
        geometry=_bbox_to_geometry(bbox),
        bbox=bbox,
        datetime=None,
        start_datetime=start_dt,
        end_datetime=end_dt,
        properties={
            "title": item_title,
            "description": entry.get("Description", ""),
        },
        stac_extensions=ICECHUNK_STAC_EXTENSIONS,
    )

    item.extra_fields["storage:schemes"] = {
        f"aws-s3-{bucket}": {
            "type": "aws-s3",
            "bucket": bucket,
            "region": region,
            "anonymous": True,
        }
    }

    item.add_asset(
        f"data@{snap}",
        pystac.Asset(
            href=store_uri,
            media_type=ICECHUNK_MEDIA_TYPE,
            roles=["data", "references"],
            extra_fields={
                "icechunk:snapshot_id": snap,
                "version": snap,
                "storage:refs": [f"aws-s3-{bucket}"],
            },
        ),
    )

    # Datacube extension via xstac
    try:
        import xstac
        item = xstac.xarray_to_stac(
            ds,
            item,
            temporal_dimension=time_dim,
            x_dimension=x_dim,
            y_dimension=y_dim,
            reference_system=ref_sys,
            validate=False,
        )
    except Exception:
        # xstac not installed or failed — manual fallback
        time_min = start_dt.isoformat() + "Z" if start_dt else None
        time_max = end_dt.isoformat() + "Z" if end_dt else None
        item.properties["cube:dimensions"] = {}
        if time_min and time_max:
            item.properties["cube:dimensions"][time_dim] = {
                "type": "temporal",
                "extent": [time_min, time_max],
            }
        item.properties["cube:variables"] = build_cube_variables(ds)

    # Add all remaining dims (lead_time, ensemble_member, etc.)
    skip_dims = {d for d in [time_dim, x_dim, y_dim] if d is not None}
    add_extra_dimensions(item, ds, skip_dims)

    log.info("  Built item: %s  bbox=%s", item_id, bbox)
    return item


# ---------------------------------------------------------------------------
# Catalog assembly
# ---------------------------------------------------------------------------

def build_catalog() -> pystac.Catalog:
    catalog = pystac.Catalog(
        id="dynamical-org-icechunk",
        description=(
            "Weather forecast and analysis datasets from dynamical.org, "
            "stored as Icechunk repositories on AWS S3."
        ),
        catalog_type=pystac.CatalogType.SELF_CONTAINED,
    )

    for entry in fetch_registry_entries():
        try:
            bucket, region = bucket_from_entry(entry)
        except ValueError as exc:
            log.warning("%s — skipping", exc)
            continue

        log.info("\nScanning s3://%s (%s) ...", bucket, entry.get("Name", "?"))
        for prefix in discover_icechunk_prefixes(bucket, region):
            item = build_item_for_store(bucket, prefix, region, entry)
            if item:
                catalog.add_item(item)

    return catalog


# ---------------------------------------------------------------------------
# Save + upload
# ---------------------------------------------------------------------------

def save_locally(catalog: pystac.Catalog, output_dir: Path, public_domain: str, catalog_prefix: str) -> None:
    catalog_url = f"https://{public_domain}/{catalog_prefix}"
    catalog.normalize_hrefs(catalog_url)
    # Save with local paths for upload
    catalog.normalize_hrefs(str(output_dir))
    catalog.save()
    log.info("Catalog saved to: %s (%d items)", output_dir, len(list(catalog.get_items())))


def upload_to_s3(output_dir: Path, bucket: str, prefix: str, profile: str) -> None:
    fs = s3fs.S3FileSystem(profile=profile)
    log.info("Uploading to s3://%s/%s ...", bucket, prefix)
    for local_file in sorted(output_dir.rglob("*.json")):
        rel = local_file.relative_to(output_dir)
        dest = f"{bucket}/{prefix}/{rel}"
        fs.put(str(local_file), dest)
        log.info("  %s → s3://%s", rel, dest)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog-bucket",  default="osc-pub")
    parser.add_argument("--catalog-prefix",  default="stac/dynamical")
    parser.add_argument("--profile",         default="osc-pub-r2",
                        help="AWS profile with write credentials for catalog bucket")
    parser.add_argument("--public-domain",   default="r2-pub.openscicomp.io")
    parser.add_argument("--output-dir",      type=Path, default=None)
    parser.add_argument("--no-upload",       action="store_true")
    parser.add_argument("-v", "--verbose",   action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    output_dir = args.output_dir or Path(tempfile.mkdtemp(prefix="dynamical-stac-"))
    output_dir.mkdir(parents=True, exist_ok=True)

    catalog = build_catalog()

    n = len(list(catalog.get_items()))
    if n == 0:
        log.error("No STAC items built — aborting.")
        sys.exit(1)

    save_locally(catalog, output_dir, args.public_domain, args.catalog_prefix)

    if args.no_upload:
        log.info("--no-upload set, skipping S3 upload.")
    else:
        upload_to_s3(output_dir, args.catalog_bucket, args.catalog_prefix, args.profile)

    catalog_url = f"https://{args.public_domain}/{args.catalog_prefix}/catalog.json"
    print(f"\nCatalog URL:  {catalog_url}")
    print(
        "STAC Browser: https://radiantearth.github.io/stac-browser/#/external/"
        + catalog_url
    )


if __name__ == "__main__":
    main()
