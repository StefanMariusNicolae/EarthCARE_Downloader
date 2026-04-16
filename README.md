# EarthCARE Downloader

A Python module to query and download EarthCARE Level 1 and Level 2 files from ESA's MAAP Catalogue using STAC API.

## Installation

1. Clone this repository.
2. Install the package:
   ```bash
   pip install .
   ```

## Configuration

The downloader uses two configuration files located in the `configs/` directory:

1. `config.yml`: Contains search parameters such as:
   - `collections`: List of STAC collections to search.
   - `start_date` / `end_date`: Temporal filtering.
   - `bbox`: Bounding box filtering `[min_lon, min_lat, max_lon, max_lat]`.
   - `download_folder`: Path where files will be saved.
   - `max_items`: Maximum number of items to download.

2. `credentials.yaml`: Contains API credentials:
   - `stac_token`: Your MAAP API token.

## Usage

You can run the downloader directly:

```bash
python src/downloader.py
```

Or use it as a module:

```python
from src.downloader import EarthCAREDownloader

downloader = EarthCAREDownloader()
items = downloader.query_products()
downloader.download_items(items)
```

## API Details

- STAC Endpoint: `https://catalog.maap.eo.esa.int/catalogue/stac`
- Default Collections: `EarthCAREL1Validated_MAAP`, `EarthCAREL2Validate_MAAP`
