# EarthCARE Downloader

A Python module to query and download EarthCARE Level 1 and Level 2 files from ESA's MAAP Catalogue using STAC API.

## Features

- Query EarthCARE data using STAC API.
- Support for various product types (e.g., `ATL_NOM_1B`, `ATL_EBD_2A`, `ATL_AER_2A`).
- Temporal and spatial filtering (Bounding Box or Point-based).
- Parallel downloads for improved performance.
- Automatic unzipping and cleanup of downloaded files.
- Metadata export to CSV.

## Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd EarthCARE_Downloader
   ```

2. Install the package and its dependencies:
   ```bash
   pip install .
   ```

## Configuration

The downloader is configured using the `configs/config.yml` file.

### Search Parameters

- `offline_token`: Your offline API token obtained from the [ESA MAAP Portal](https://iam.maap.eo.esa.int/realms/esa-maap/protocol/openid-connect/auth?response_type=code&scope=openid&client_id=esa-maap-portal).
- `product_type`: A list of EarthCARE product types to download (e.g., `ATL_NOM_1B`, `ATL_EBD_2A`).
- `start_date` / `end_date`: Date range for filtering in ISO 8601 format (`%Y-%m-%dT%H:%M:%SZ`).
- `bbox`: Bounding box for filtering `[min_lon, min_lat, max_lon, max_lat]`.
- `distance_from_point`: Shorthand for spatial filtering `[lon, lat, delta_lon, delta_lat]`. Takes priority over `bbox`.

### Download Settings

- `download_folder`: Absolute path where files will be saved.
- `max_items`: Limits the number of items retrieved from the API per product type.
- `unzip_files`: Boolean, if `True` it will unzip `.zip` files after downloading.
- `delete_zips`: Boolean, if `True` it will delete `.zip` files after unarchiving (only if `unzip_files` is `True`).
- `save_download_metadata_csv`: Boolean, if `True` it saves a CSV file with download details.
- `number_of_parallel_downloads`: Maximum number of parallel downloads (hard limit of 64). Set to `1` or `None` for sequential download.
- `overwrite_existing_files`: Boolean, if `True` it will overwrite existing files in the download folder.

## Usage

### Running as a script

You can run the downloader directly using the configuration from `configs/config.yml`:

```bash
python src/downloader.py
```

### Using as a module

You can also integrate `EarthCAREDownloader` into your own Python scripts:

```python
from src.downloader import EarthCAREDownloader

# Initialize the downloader (uses default config path)
downloader = EarthCAREDownloader()

# Search for products based on config or overrides
items = downloader.search()

# Download the found items
downloader.download()
```

## API Details

- **STAC Endpoint**: `https://catalog.maap.eo.esa.int/catalogue/stac`
- **Official Documentation**: [ESA MAAP Portal](https://portal.maap.eo.esa.int/)
