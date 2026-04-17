### This file contains all the constants used in the program

import os

STAC_URL = "https://catalog.maap.eo.esa.int/catalogue/"
IAM_URL = "https://iam.maap.eo.esa.int/realms/esa-maap/protocol/openid-connect/token"
EARTHCARE_COLLECTIONS = ["EarthCAREL1Validated_MAAP", "EarthCAREL2Validated_MAAP"]
CLIENT_ID = "offline-token"
CLIENT_SECRET = "p1eL7uonXs6MDxtGbgKdPVRAmnGxHpVE"
ROOT_PROJECT_PATH = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))