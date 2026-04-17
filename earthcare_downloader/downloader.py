import os
import requests
from loguru import logger
from pystac_client import Client
from tqdm import tqdm
import sys
from earthcare_downloader.constants import STAC_URL, EARTHCARE_COLLECTIONS, IAM_URL, CLIENT_ID, CLIENT_SECRET, ROOT_PROJECT_PATH
from earthcare_downloader.config_loader import get_config
import pandas as pd
import multiprocessing as mp
import zipfile
from functools import partial
import getpass

logger.remove()
logger.add(sys.stderr, level="INFO", format="[{level}] {time}: {message}", colorize=True)

class EarthCAREDownloader:
    def __init__(self, config_path=os.path.join(ROOT_PROJECT_PATH, "configs", "default.yml"), no_download=False):
        """
        Initializes an instance with configuration, authentication, logging, directory setup, and
        various download options for managing data workflow and interactions with STAC services.

        Provided user methods are:
            - set_config: Overwrites the default configuration with a new one, either through a new path or by kwargs.
                e.g. EarthCAREDownloader.set_config(config_path="new_config.yml")
                e.g. EarthCAREDownloader.set_config(bbox=[-122.4, 37.8, -122.3, 37.9], no_download=True, download_dir="new_dir"))
            - list_config: Lists all properties of the EarthCAREDownloader object that are useful for searching and downloading.
                Example output for the default config:
                    2026-04-17T06:42:04.976859+0000 ERROR STAC token not found in credentials. You will not be able to download data!  -  because no token was declared in the config file.
                    2026-04-17T06:42:05.107471+0000 INFO Default start date is: 2025-05-16T00:00:00Z  -  this is the default start date for the EarthCARE query in the config file.
                    2026-04-17T06:42:05.109196+0000 INFO Default end date is: 2025-05-30T23:59:59Z  -  this is the default end date for the EarthCARE query in the config file.
                    2026-04-17T06:42:05.110202+0000 INFO self.bbox=[25.01, 43.24, 27.01, 45.24]  -  this is the bounding box for the EarthCARE query in the config file.
                    2026-04-17T06:42:05.111260+0000 INFO self.save_download_metadata_csv=True  -  whether or not to save the download_metadata.csv file (which shows what files should be downloaded)
                    2026-04-17T06:42:05.112204+0000 INFO self.can_download=False  -  whether or not the EarthCARE query can be downloaded. Either user preference or consequence of missing token
                    2026-04-17T06:42:05.113206+0000 INFO self.download_dir='/workspace/DIVA_Bucharest_Workshop/EarthCARE_Downloader/output/data'  -  the directory where files will be downloaded
                    2026-04-17T06:42:05.114116+0000 INFO self.overwrite_cache=False  -  whether or not to overwrite existing files in the download directory
                    2026-04-17T06:42:05.115032+0000 INFO self.unzip_files=True  -  whether or not to unzip downloaded .zip files
                    2026-04-17T06:42:05.115936+0000 INFO self.delete_zips=True  -  whether or not to delete .zip files after unzipping (only takes effect if self.unzip_files=True)
                    2026-04-17T06:42:05.116896+0000 INFO self.max_items=None  -  maximum number of items to retrieve from the API (user will be warned if they are missing data from queries)
                    2026-04-17T06:42:05.117789+0000 INFO self.max_download_workers=12  -  maximum number of parallel download processes to spawn. Hard cap of 64 processes.
                    2026-04-17T06:42:05.119357+0000 INFO self.parallel_download=True  -  whether or not to use parallel download processes
            - search: Performs a search for items matching the specified criteria. 'start_date', 'end_date' and 'bbox'
                      can be overwritten from the ones declared in the config file if kwargs are provided.
                e.g. EarthCAREDownloader.search()
                e.g. EarthCAREDownloader.search(start_date="2026-02-01", end_date="2026-02-15", bbox=[-122.4, 37.8, -122.3, 37.9])
            - save_download_metadata: Saves the search results to a CSV file. Either uses the cached search results from running .search()
                                      or similarly formatted dictionary, that is returned by the .search() function (for specific query).
                e.g. EarthCAREDownloader.save_download_metadata()
                e.g. EarthCAREDownloader.save_download_metadata(items=EarthCAREDownloader.search(start_date="2026-02-01", end_date="2026-02-15", bbox=[-122.4, 37.8, -122.3, 37.9]))
            - download: Downloads the files specified in the 'items' parameter. If no 'items' are given, all cached files from the previous queries will be downloaded.
                        Additional params:
                            - 'silent' (bool, default False) prevents console output
                            - 'disable_progress_bar' (bool) disables the progress bar for each file. False if single-threaded, True if multi-threaded.
                            - 'overwrite_cache' (bool, default from config file) overwrites existing files in the download directory.
                e.g. EarthCAREDownloader.download()
                e.g. EarthCAREDownloader.download(items=EarthCAREDownloader.search(start_date="2026-02-01", end_date="2026-02-15", bbox=[-122.4, 37.8, -122.3, 37.9]))
                e.g. EarthCAREDownloader.download(silent=True, disasble_progress_bar=True, overwrite_cache=True))

        :param config_path: The path to the YAML configuration file defining settings for
            the instance. Defaults to a file, "default.yml", located in a "configs" folder
            under the project's root directory.
        :type config_path: str or PathLike
        :param no_download: Specifies whether downloads should be skipped. When set to
            True, download-related functionalities will be disabled. Defaults to False.
        :type no_download: bool
        """
        self.config = get_config(config_path)
        self.stac_url = STAC_URL
        self.collections = EARTHCARE_COLLECTIONS
        self.logger = logger
        self.client = Client.open(self.stac_url)  # Opens the STAC catalogue in its root

        # Reading the config file for different params
        self.offline_token = self.config.get("offline_token", "YOUR_TOKEN_HERE")
        self.bbox = self._get_geo_filter()
        self.product_types = self.config.get("product_type", [])
        if not isinstance(self.product_types, list):
            self.product_types = [self.product_types]

        # Get access token if downloading is required
        self.token = None
        if self.offline_token == "YOUR_TOKEN_HERE":
            logger.warning("STAC token not found in credentials. You will not be able to download data!")
            self.can_download = False
        elif not no_download:
            self.token = self._get_access_token()
            self.can_download = True
        if no_download:
            logger.info("Selecting 'no_download' option, skipping download(s).")
            self.token = None
            self.can_download = False

        # Sets ancillary parameters for downloading
        if self.config.get("download_folder", "") == "":
            self.download_dir = os.path.join(ROOT_PROJECT_PATH, "output", "data")
        else:
            self.download_dir = self.config.get("download_folder")
        self.overwrite_cache = self.config.get("overwrite_existing_files", False)
        self.unzip_files = self.config.get("unzip_files", True)
        self.delete_zips = self.config.get("delete_zips", True)
        if not self.unzip_files and self.delete_zips:
            logger.warning("Unzipping files is disabled, but deleting zips is enabled. .zip files will not be deleted.")
            self.delete_zips = False
        if self.download_dir == "":
            self.download_dir = os.path.join(ROOT_PROJECT_PATH, "output", "data")
        # Sets maximum number of items to be downloaded, default is None (no limit)
        self.max_items = self.config.get("max_items", None)
        self.max_download_workers = self.config.get("number_of_parallel_downloads", 4)
        self.save_download_metadata_csv = self.config.get("save_download_metadata_csv", False)
        if self.max_download_workers is None or self.max_download_workers == 1:
            self.parallel_download = False
        else:
            self.parallel_download = True
        if self.max_download_workers > 64:
            logger.warning("Number of parallel downloads is set to a very high value. Will default to 64 processes.")
            self.max_download_workers = 64

        # Initialize and sanitize internal variables
        self._search_results = {}
        self._warned_about_missing_data = False

        # Make sure download directory exists
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
        logger.success("Access token retrieved successfully.")

        if not access_token:
            logger.error("Failed to retrieve access token from authentication server. Will revert to no download!")
            self.can_download = False
            self.token = None

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
        self._search_results = {}

    @staticmethod
    def _download_file(url, filename, product_type, download_dir, token, unzip, delete_zips, disable_progress_bar=False,
                       silent=False, overwrite_cache=False):
        """Helper to download a single file with or without progress bar."""

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

    def set_token(self, offline_token=None, no_download=False):
        """
        Sets the offline token and determines the download permissions based on the
        provided token. Set 'no_download' to True to disable downloading.

        :param offline_token: The token required for authentication. Provide a valid
            offline token as obtained from the ESA MAAP Portal.
        :type offline_token: str
        :param no_download: Optional flag indicating whether downloading should be
            disabled regardless of token validity. Defaults to False.
        :type no_download: bool
        :return: None
        """
        if offline_token is None:
            offline_token = getpass.getpass(prompt="Please enter your ESA MAAP Portal offline token:")
        self.offline_token = offline_token
        if self.offline_token == "YOUR_TOKEN_HERE":
            logger.warning("STAC token not found in credentials. You will not be able to download data!")
            self.token = None
            self.can_download = False
        elif not no_download:
            self.token = self._get_access_token()
            self.can_download = True

    def set_config(self, config_path=None, no_download=False, bbox=None, save_download_metadata_csv=None,
                   download_dir=None, overwrite_cache=None, unzip_files=None, delete_zips=None,
                   max_items=None, max_download_workers=None, product_types=None):
        """
        Convenience function to overwrite configs after the downloader has been initialized. All parameters are optional
            and kwargs take priority over the config file (if provided).
        NOTE: Changing the config resets the internal search results cache, as it is no longer valid.

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
        :param product_types: List of product types to download
        :return: None
        """
        # Check if the config file is provided and load it if it is. If not, fallback onto original settings
        if config_path is not None:
            self.config = get_config(config_path)

        if bbox is not None:
            self.bbox = bbox
        else:
            self.bbox = self._get_geo_filter()

        if product_types is not None:
            self.product_types = product_types
        else:
            self.product_types = self.config.get("product_type", [])
        if not isinstance(self.product_types, list):
            self.product_types = [self.product_types]

        # Token check still has to be done, especially if config file changed!
        self.token = None
        
        if save_download_metadata_csv is not None:
            self.save_download_metadata_csv = save_download_metadata_csv
        else:
            self.save_download_metadata_csv = self.config.get("save_download_metadata_csv", False)

        # Here we assume that the token is already declared from the __init__ call. No need to redeclare it!
        if self.offline_token == "YOUR_TOKEN_HERE":
            logger.warning("STAC token not found in credentials. You will not be able to download data!")
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
        """
        Logs the current configuration settings of the instance.

        This method outputs the instance's configuration settings such as the
        default start and end dates, bounding box information, download directory,
        flags for managing metadata, and other related configuration options to
        the logger, primarily for debugging and information purposes.

        :return: None
        """
        self.logger.info(f"Default start date is: {self.config.get('start_date')}")
        self.logger.info(f"Default end date is: {self.config.get('end_date')}")
        self.logger.info(f"{self.bbox=}")
        self.logger.info(f"{self.product_types=}")
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
        """
        Search for products within specified configurations and filters, and returns the
        matching items. The method handles configurations for product types, datetime filters,
        and bounding boxes. If `start_date`, `end_date`, or `bbox` are not provided, it
        defaults to the configurations defined in the class object. Searches are performed for
        each product type, and results are cached before being returned.

        :param start_date: The start of the date range for the search. Defaults to a class-wide
            configuration setting if not provided.
            Format expected: datetime-like str (pref. ISO8601 format) or datetime.
        :param end_date: The end of the date range for the search. Defaults to a class-wide
            configuration setting if not provided.
            Format expected: datetime-like str (pref. ISO8601 format) or datetime..
        :param bbox: Bounding box (min_lon, min_lat, max_lon, max_lat)
            coordinates to spatially limit the search. Defaults to a
            class-wide configuration setting if not provided.
        :return: A dictionary of items found during the search operation.
            Keys are product types, and values are lists of matching items.
        """
        product_types = self.product_types
        
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

            if len(items) != results and self._warned_about_missing_data:
                logger.warning(
                    f"Number of earch results for {product_type} products do not match the number of items returned.\n"
                    f"Consider increasing 'max_items' in the config or using a more specific datetime filter, else data will be lost when downloading."
                )
                self._warned_about_missing_data = True

            # Append to global _search_results only if reasonable number of items are found
            if len(items) < 100:
                if self._search_results.get(product_type, None) is not None:
                    self._search_results[product_type].extend(items)
                else:
                    self._search_results[product_type] = items
                logger.info(f"Found {len(items)} items matching the criteria.")

        return items

    def save_download_metadata(self, items=None):
        """
        Save metadata of downloaded items to a CSV file.

        This method generates a CSV file containing metadata such as download
        URLs, filenames, product types, start datetime, and end datetime for
        the given items. If no items are provided, it uses the default search
        results. The file is saved in the download directory with a timestamped
        filename.

        :param items: List of items for which metadata should be saved.
                      Defaults to None, which uses cached search results.
        :type items: Optional[List[Any]]
        :return: None
        """

        csv_file_path = os.path.join(self.download_dir, f"download_metadata_{pd.Timestamp.now().strftime('%Y-%m-%dT%H-%M-%S')}.csv")

        logger.info(f"Saving download metadata to {csv_file_path}...")

        if items is None:
            items = self._search_results

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
        """
        Download specified items or all items in the search results, handling metadata preparation,
        progress bar configuration, and parallel or sequential downloading.

        If no items are specified, this method will attempt to download all cached results from previous queries.

        :param items: List of items to be downloaded. Defaults to None, which means
            all search results will be downloaded.
        :type items: list, optional
        :param silent: If set to True, suppresses most logging output during
            the download process. Defaults to None, which preserves the original
            verbosity setting.
        :type silent: bool, optional
        :param disable_progress_bar: If set to True, disables the progress bar during
            the download process. Defaults to None, which retains the existing
            progress bar setting for single-threaded operation. The progress bar is
            automatically disabled for parallel processing.
        :type disable_progress_bar: bool, optional
        :param overwrite_cache: If set to True, overwrites local cached files
            during the download process. Defaults to None, which uses the instance's
            existing cache overwriting policy from the config file.
        :type overwrite_cache: bool, optional
        :return: None
        """

        if not self.can_download:
            logger.error("Cannot download data, skipping download.")
            return

        if items is None:
            items = self._search_results

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
                            disable_progress_bar=disable_progress_bar if disable_progress_bar is not None else True,
                            silent=silent if silent is not None else False,
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
                    silent=silent if silent is not None else False,
                    overwrite_cache=self.overwrite_cache if overwrite_cache is None else overwrite_cache
                )
        logger.success("Download finished successfully!")

if __name__ == "__main__":
    downloader = EarthCAREDownloader()
    found_items = downloader.search()  # Query with the default settings in config file
    downloader.download(items=found_items)
    restricted_search_items = downloader.search(start_date="2026-02-01", end_date="2026-02-02")
    downloader.save_download_metadata(restricted_search_items)

    # Download all cached items, from both queries
    downloader.download()
