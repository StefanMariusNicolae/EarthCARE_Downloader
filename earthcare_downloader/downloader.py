import os
import requests
from loguru import logger
from pystac_client import Client
from tqdm import tqdm
import sys
from earthcare_downloader.constants import STAC_URL, EARTHCARE_COLLECTIONS, IAM_URL, CLIENT_ID, CLIENT_SECRET
from earthcare_downloader.config_loader import get_config
import pandas as pd
import multiprocessing as mp
import zipfile
from functools import partial

logger.remove()
logger.add(sys.stderr, level="INFO", format="{time} {level} {message}", colorize=True)
ROOT_PROJECT_PATH = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))

class EarthCAREDownloader:
    def __init__(self, config_path=os.path.join(ROOT_PROJECT_PATH, "configs", "config.yml"), no_download=False):
        self.config = get_config(config_path)
        self.stac_url = STAC_URL
        self.collections = EARTHCARE_COLLECTIONS
        self.offline_token = self.config.get("offline_token", "YOUR_TOKEN_HERE")
        self.logger = logger
        self.bbox = self._get_geo_filter()
        self.token = None
        self.save_download_metadata_csv = self.config.get("save_download_metadata_csv", False)
        if self.offline_token == "YOUR_TOKEN_HERE":
            logger.error("STAC token not found in credentials. You will not be able to download data!")
            self.can_download = False
        elif not no_download:
            self.token = self._get_access_token()
            self.can_download = True
        if no_download:
            logger.info("Selecting 'no_download' option, skipping download(s).")
            self.token = None
            self.can_download = False
        self.download_dir = self.config.get("download_folder", os.path.join(ROOT_PROJECT_PATH, "data"))
        self.overwrite_cache = self.config.get("overwrite_existing_files", False)
        self.unzip_files = self.config.get("unzip_files", True)
        self.delete_zips = self.config.get("delete_zips", True)
        if not self.unzip_files and self.delete_zips:
            logger.warning("Unzipping files is disabled, but deleting zips is enabled. .zip files will not be deleted.")
            self.delete_zips = False
        if self.download_dir == "":
            self.download_dir = os.path.join(ROOT_PROJECT_PATH, "output", "data")
        # Opens the STAC catalogue in its root
        self.client = Client.open(self.stac_url)
        # Sets maximum number of items to be downloaded, default is None (no limit)
        self.max_items = self.config.get("max_items", None)
        self.max_download_workers = self.config.get("number_of_parallel_downloads", 4)
        if self.max_download_workers is None or self.max_download_workers == 1:
            self.parallel_download = False
        else:
            self.parallel_download = True
        if self.max_download_workers > 64:
            logger.warning("Number of parallel downloads is set to a very high value. Will default to 64 processes.")
            self.max_download_workers = 64
        self.search_results = {}
        self.warned_about_missing_data = False
        # self.max_items = 10
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def _get_access_token(self):
        """Retrieves access token from credentials if offline_token is available."""

        url = IAM_URL
        data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": self.offline_token,
            "scope": "offline_access openid"
        }

        response = requests.post(url, data=data)
        response.raise_for_status()
        access_token = response.json().get("access_token")
        logger.info("Access token retrieved successfully.")

        if not access_token:
            raise RuntimeError("Failed to retrieve access token.")

        return access_token

    @staticmethod
    def _get_datetime_filter(start_date, end_date):
        if start_date:
            start = pd.Timestamp(start_date, tzinfo=None).isoformat(timespec='milliseconds')[:-4] + "Z"
        else:
            start = ".."
        if end_date:
            end = pd.Timestamp(end_date, tzinfo=None).isoformat(timespec='milliseconds')[:-4] + "Z"
        else:
            end = ".."
        datetime_filter = f"{start}/{end}" if start and end else (start or end)
        return datetime_filter

    def _get_geo_filter(self):
        if self.config.get("bbox") and self.config.get("distance_from_point"):
            logger.info("Both bbox and distance_from_point are provided, using 'distance_from_point' filter.")
            distance_from_point_list = self.config.get("distance_from_point")
            bbox = [distance_from_point_list[0] - distance_from_point_list[2],
                    distance_from_point_list[1] - distance_from_point_list[3],
                    distance_from_point_list[0] + distance_from_point_list[2],
                    distance_from_point_list[1] + distance_from_point_list[3]]
        elif self.config.get("bbox"):
            bbox = self.config.get("bbox")
        elif self.config.get("distance_from_point"):
            distance_from_point_list = self.config.get("distance_from_point")
            bbox = [distance_from_point_list[0] - distance_from_point_list[2],
                    distance_from_point_list[1] - distance_from_point_list[3],
                    distance_from_point_list[0] + distance_from_point_list[2],
                    distance_from_point_list[1] + distance_from_point_list[3]]
        else:
            logger.error("Please provide a bbox or distance_from_point to filter by.")
            raise ValueError("Please provide a bbox or distance_from_point to filter by.")
        return bbox

    def _one_search(self, product_type, datetime_filter, bbox):

        search_result = self.client.search(
            filter=f"productType = '{product_type}'",
            collections=self.collections,
            datetime=datetime_filter,
            max_items=self.max_items,
            method="GET",
            bbox=bbox,
        )

        return search_result

    def _delete_internal_cached_results(self):
        self.search_results = {}

    @staticmethod
    def _download_file(url, filename, product_type, download_dir, token, unzip, delete_zips, disable_progress_bar=False,
                       silent=False, overwrite_cache=False):
        """Helper to download a single file with progress bar."""

        folder_path = os.path.join(download_dir, product_type)
        file_path = os.path.join(folder_path, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if not overwrite_cache:  # Check to see if file exists only if 'overwrite_cache' is False
            if os.path.exists(file_path):  # Check for .zip file first
                logger.info(f"Zip file '{filename}' already exists, skipping.")
                return
            if os.path.exists(file_path[:-4]):  # Then check for unzipped file
                logger.info(f"Unzipped file (folder) '{filename[:-4]}' already exists, skipping.")
                return

        if not silent:
            logger.info(f"Downloading {filename}...")

        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            if disable_progress_bar:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            else:
                with open(file_path, 'wb') as f, tqdm(
                        total=total_size, unit='B', unit_scale=True, desc=filename
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            if not silent:
                logger.info(f"Successfully downloaded {filename}")
            if unzip:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(path=folder_path)
                if not silent:
                    logger.info(f"Successfully unzipped {filename}")
                if delete_zips:
                    os.remove(file_path)
                    if not silent:
                        logger.info(f"Successfully deleted {filename}")

        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")

    @staticmethod
    def _prepare_download_metadata(items):

        product_types = list(items.keys())

        download_urls = []
        filenames = []
        download_product_types = []
        start_datetimes = []
        end_datetimes = []

        # Prepare items for downloading
        for product_type in product_types:
            for item in items[product_type]:
                # Identify data assets - typically 'data' or specific file types
                download_url = item.assets["product"].get_absolute_href()
                filename = download_url.split('/')[-1] + ".zip"
                start_datetime = item.properties.get("start_datetime")
                end_datetime = item.properties.get("end_datetime")

                download_urls.append(download_url)
                filenames.append(filename)
                download_product_types.append(product_type)
                start_datetimes.append(start_datetime)
                end_datetimes.append(end_datetime)

        return download_urls, filenames, download_product_types, start_datetimes, end_datetimes

    def set_config(self, config_path=None, no_download=False, bbox=None, save_download_metadata_csv=None,
                   download_dir=None, overwrite_cache=None, unzip_files=None, delete_zips=None,
                   max_items=None, max_download_workers=None):
        """
        Convenience function to overwrite configs after the downloader has been initialized. All parameters are optional
            and kwargs take priority over the config file (if provided).
        :param config_path: Path to the new config file
        :param no_download: Whether to skip downloading data
        :param bbox: Bounding box for spatial filtering [min_lon, min_lat, max_lon, max_lat]
        :param save_download_metadata_csv: Whether to save download metadata to a CSV file
        :param download_dir: Absolute path to the directory where files will be saved
        :param overwrite_cache: Whether to overwrite existing files in the download directory
        :param unzip_files: Whether to unzip downloaded .zip files
        :param delete_zips: Whether to delete .zip files after unzipping
        :param max_items: Maximum number of items to retrieve from the API
        :param max_download_workers: Number of parallel download processes
        :return: None
        """

        # Check if the config file is provided and load it if it is. If not, fallback onto original settings
        if config_path is not None:
            self.config = get_config(config_path)

        if bbox is not None:
            self.bbox = bbox
        else:
            self.bbox = self._get_geo_filter()

        # Token check still has to be done, especially if config file changed!
        self.token = None
        
        if save_download_metadata_csv is not None:
            self.save_download_metadata_csv = save_download_metadata_csv
        else:
            self.save_download_metadata_csv = self.config.get("save_download_metadata_csv", False)

        # Here we assume that the token is already declared from the __init__ call. No need to redeclare it!
        if self.offline_token == "YOUR_TOKEN_HERE":
            logger.error("STAC token not found in credentials. You will not be able to download data!")
            self.can_download = False
        elif not no_download:
            self.token = self._get_access_token()
            self.can_download = True
        
        if no_download:
            logger.info("Selecting 'no_download' option, skipping download(s).")
            self.token = None
            self.can_download = False

        if download_dir is not None:
            self.download_dir = download_dir
        else:
            self.download_dir = self.config.get("download_folder", os.path.join(ROOT_PROJECT_PATH, "output", "data"))
            if self.download_dir == "":
                self.download_dir = os.path.join(ROOT_PROJECT_PATH, "output", "data")

        if overwrite_cache is not None:
            self.overwrite_cache = overwrite_cache
        else:
            self.overwrite_cache = self.config.get("overwrite_existing_files", False)

        if unzip_files is not None:
            self.unzip_files = unzip_files
        else:
            self.unzip_files = self.config.get("unzip_files", True)

        if delete_zips is not None:
            self.delete_zips = delete_zips
        else:
            self.delete_zips = self.config.get("delete_zips", True)

        if not self.unzip_files and self.delete_zips:
            logger.warning("Unzipping files is disabled, but deleting zips is enabled. .zip files will not be deleted.")
            self.delete_zips = False

        # Opens the STAC catalogue in its root
        if self.client is None:
            self.client = Client.open(self.stac_url)

        # Sets maximum number of items to be downloaded, default is None (no limit)
        if max_items is not None:
            self.max_items = max_items
        else:
            self.max_items = self.config.get("max_items", None)

        if max_download_workers is not None:
            self.max_download_workers = max_download_workers
        else:
            self.max_download_workers = self.config.get("number_of_parallel_downloads", 4)

        if self.max_download_workers is None or self.max_download_workers <= 1:
            self.parallel_download = False
            self.max_download_workers = 1
        else:
            self.parallel_download = True

        if self.max_download_workers > 64:
            logger.warning("Number of parallel downloads is set to a very high value. Will default to 64 processes.")
            self.max_download_workers = 64
        
        self._delete_internal_cached_results()
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def list_config(self):
        """Lists all properties of the EarthCAREDownloader object."""
        self.logger.info(f"Default start date is: {self.config.get('start_date')}")
        self.logger.info(f"Default end date is: {self.config.get('end_date')}")
        self.logger.info(f"{self.bbox=}")
        self.logger.info(f"{self.save_download_metadata_csv=}")
        self.logger.info(f"{self.can_download=}")
        self.logger.info(f"{self.download_dir=}")
        self.logger.info(f"{self.overwrite_cache=}")
        self.logger.info(f"{self.unzip_files=}")
        self.logger.info(f"{self.delete_zips=}")
        self.logger.info(f"{self.max_items=}")
        self.logger.info(f"{self.max_download_workers=}")
        self.logger.info(f"{self.parallel_download=}")

    def search(self, start_date=None, end_date=None, bbox=None):
        """Queries EarthCARE L1 and L2 products based on config."""
        product_types = self.config.get("product_type")

        if not isinstance(product_types, list):
            product_types = [product_types]
        
        # Datetime handling: combine start and end or use a single string
        if not start_date:
            start_date = self.config.get("start_date")
        if not end_date:
            end_date = self.config.get("end_date")
        if not bbox:
            bbox = self.bbox

        datetime_filter = self._get_datetime_filter(start_date, end_date)

        items = {}

        for product_type in product_types:
            logger.info(f"Searching for {product_type} products...")
            logger.info(f"Filters - Datetime: {datetime_filter}, BBOX: {bbox}")

            search_result = self._one_search(product_type, datetime_filter, bbox)
            items = list(search_result.items())
            results = search_result.matched()

            if len(items) != results and self.warned_about_missing_data:
                logger.warning(
                    f"Number of earch results for {product_type} products do not match the number of items returned.\n"
                    f"Consider increasing 'max_items' in the config or using a more specific datetime filter, else data will be lost when downloading."
                )
                self.warned_about_missing_data = True

            # Append to global search_results only if reasonable number of items are found
            if len(items) < 100:
                if self.search_results.get(product_type, None) is not None:
                    self.search_results[product_type].extend(items)
                else:
                    self.search_results[product_type] = items
                logger.info(f"Found {len(items)} items matching the criteria.")

        return items

    def save_download_metadata(self, items=None):
        """Saves metadata of the downloaded items to a CSV file."""

        csv_file_path = os.path.join(self.download_dir, f"download_metadata_{pd.Timestamp.now().strftime('%Y-%m-%dT%H-%M-%S')}.csv")

        logger.info(f"Saving download metadata to {csv_file_path}...")

        if items is None:
            items = self.search_results

        download_urls, filenames, download_product_types, start_datetimes, end_datetimes = self._prepare_download_metadata(items)
        download_metadata = pd.DataFrame({
            "download_url": download_urls,
            "filename": filenames,
            "product_type": download_product_types,
            "start_datetime": start_datetimes,
            "end_datetime": end_datetimes
        })
        download_metadata.to_csv(csv_file_path, index=False)


    def download(self, items=None, silent=None, disable_progress_bar=None, overwrite_cache=None):
        """Downloads assets for the given list of STAC items."""

        if not self.can_download:
            logger.error("Cannot download data, skipping download.")
            return

        if items is None:
            items = self.search_results

        logger.info(f"Preparing products for download...")
        download_urls, filenames, download_product_types, start_datetimes, end_datetimes = self._prepare_download_metadata(items)

        if self.save_download_metadata_csv:
            self.save_download_metadata(items)

        if self.parallel_download:
            no_of_workers = min(self.max_download_workers, len(download_urls))
            if disable_progress_bar is False or disable_progress_bar is None:
                logger.warning("Parallel downloading does not support a progress bar! It will be disabled.")
                disable_progress_bar = True
            # Download files in parallel
            logger.info(f"Downloading {len(download_urls)} files in parallel with {no_of_workers} processes. This can take a while...")
            with mp.Pool(processes=no_of_workers) as pool:
                pool.starmap(
                    partial(self._download_file,
                            download_dir=self.download_dir,
                            token=self.token,
                            unzip=self.unzip_files,
                            delete_zips=self.delete_zips,
                            disable_progress_bar=disable_progress_bar,
                            silent=False if silent is None else silent,
                            overwrite_cache=self.overwrite_cache if overwrite_cache is None else overwrite_cache),
                    zip(download_urls, filenames, download_product_types)
                )
        else:
            logger.info(f"Downloading {len(download_urls)} files sequentially. This can take a while...")
            # Download files sequentially
            for url, filename, product_type in zip(download_urls, filenames, download_product_types):
                self._download_file(
                    url,
                    filename,
                    product_type,
                    download_dir=self.download_dir,
                    token=self.token,
                    unzip=self.unzip_files,
                    delete_zips=self.delete_zips,
                    disable_progress_bar=disable_progress_bar if disable_progress_bar is not None else False,
                    silent=False if silent is None else silent,
                    overwrite_cache=self.overwrite_cache if overwrite_cache is None else overwrite_cache
                )
        logger.success("Download finished successfully!")

if __name__ == "__main__":
    downloader = EarthCAREDownloader()
    found_items = downloader.search()
    # downloader.save_download_metadata()
    downloader.download()
