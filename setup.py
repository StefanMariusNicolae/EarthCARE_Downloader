from setuptools import setup, find_packages

setup(
    name="earthcare-downloader",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pystac-client",
        "requests",
        "pyyaml",
        "tqdm",
        "loguru",
        'pandas',
        "cql2"
    ],
    author="Stefan Nicolae - INOE",
    description="Python module to query and download EarthCARE Level 1 and Level 2 files from ESA's MAAP Catalogue",
    python_requires=">=3.7",
)
