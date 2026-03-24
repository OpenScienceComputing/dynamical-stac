# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

```bash
# Create and activate conda environment
mamba env create -f environment.yml
conda activate dynamical-stac
```

## Running the Catalog Builder

```bash
# Run locally (outputs to a local stac/ directory, no upload)
python build_catalog.py

# Run with R2 upload (requires AWS-style credentials configured)
python build_catalog.py \
  --catalog-bucket osc-pub \
  --catalog-prefix stac/dynamical \
  --profile osc-pub-r2 \
  --public-domain r2-pub.openscicomp.io
```

There are no tests in this repository.

## Architecture

This is a single-script STAC catalog builder (`build_catalog.py`) that generates a static [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) from Icechunk stores hosted on AWS S3.

### Data Flow

1. **Discovery:** `fetch_registry_entries()` queries the AWS Open Data Registry (via GitHub API) for `dynamical-*.yaml` entries. `discover_icechunk_prefixes()` lists S3 buckets to find `.icechunk` store prefixes.

2. **Metadata extraction:** `build_item_for_store()` opens each Icechunk store via `xarray`, extracts spatial/temporal extents and variable metadata, and builds a STAC item with the datacube extension.

3. **Catalog output:** `build_catalog()` orchestrates discovery and item building. `save_locally()` writes normalized JSON files to `stac/`. `upload_to_s3()` uploads JSON to Cloudflare R2.

### Key Design Details

- STAC items use several extensions: `datacube/v2.2.0`, `zarr/v1.1.0`, `virtual-assets/v1.0.0`, `storage/v2.0.0`, `version/v1.2.0`.
- Spatial extents are reprojected to WGS84 using `rioxarray`.
- Non-spatial/non-temporal dimensions (e.g., `lead_time`, `ensemble_member`) are surfaced as `cube:dimensions` via `add_extra_dimensions()`.
- Each item includes a Python code snippet showing how to open the dataset with `xarray` + `icechunk`.
- Registry descriptions have HTML stripped via `_HTMLStripper` before inclusion in STAC metadata.

### CI/CD

GitHub Actions (`.github/workflows/build_catalog.yml`) runs the builder hourly and on manual dispatch, using secrets `R2_KEY_ID`, `R2_SECRET`, and `R2_ENDPOINT` for Cloudflare R2 uploads.
