import yaml
import os
from loguru import logger

def load_yaml(file_path):
    """Loads a YAML file."""
    if not os.path.exists(file_path):
        logger.warning(f"File {file_path} not found.")
        return {}
    try:
        with open(file_path, "r") as stream:
            return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        logger.error(f"Error loading YAML: {exc}")
        return {}

def get_config(config_path):
    """Loads the main application configuration."""
    return load_yaml(config_path)
