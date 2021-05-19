import requests
import csv
import logging

from arcgis.gis import GIS
from datetime import date
from optparse import OptionParser

from configparser import ConfigParser

parser = OptionParser()
parser.add_option("-c", "--config", dest="config", default="config.txt")
options, _ = parser.parse_args()

# get config info
config = ConfigParser()
config.read(options.config)

logger = logging.getLogger()
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

FILENAME = config.get("MISC", "FILENAME")
TEMP_PATH = f"/tmp/{FILENAME}.csv"


def fetch():
    ACLED_API_URL = config.get("ACLED", "API_URL")
    ACLED_KEY = config.get("ACLED", "KEY")
    ACLED_EMAIL = config.get("ACLED", "EMAIL")
    ISO_COUNTRIES = config.get("ACLED", "ISO_COUNTRIES").split(",")
    START_DATE = config.get("MISC", "START_DATE")

    logger.info("Fetching data from acled API")

    params = dict(
        key=ACLED_KEY,
        email=ACLED_EMAIL,
        iso="|".join(ISO_COUNTRIES),
        event_date=f"{START_DATE}|{date.today().isoformat()}",
        event_date_where="BETWEEN",
        page=1,
    )

    if len(ISO_COUNTRIES) > 1:
        params["iso_where"] = "="

    len_data = -1

    csv_file = []
    while len_data != 0:
        data = requests.get(ACLED_API_URL, params=params).json()["data"]
        csv_file.extend(data)
        len_data = len(data)
        params["page"] = params["page"] + 1

    return csv_file


def upload_arcgis():
    ARCGIS_USER = config.get("ARCGIS", "USER")
    ARCGIS_PW = config.get("ARCGIS", "PW")
    ARCGIS_URL = config.get("ARCGIS", "URL")

    gis = GIS(ARCGIS_URL, ARCGIS_USER, ARCGIS_PW)
    content_data = f"title:{FILENAME} type:CSV owner:{ARCGIS_USER}"
    items = gis.content.search(content_data)

    item_params = dict(title=FILENAME)
    overwrite = False
    if len(items) > 0:
        logger.info("Updating file in Arcgis")
        item = items[0]
        item.update(item_params, data=TEMP_PATH)
        overwrite = True
    else:
        logger.info("Uploading file to Arcgis")
        item = gis.content.add(item_params, data=TEMP_PATH)

    item.share(everyone=True)

    logger.info("Publishing layer")
    publish_params = dict(
        name=FILENAME,
        type="csv",
        locationType="coordinates",
        latitudeFieldName="latitude",
        longitudeFieldName="longitude",
    )
    item = item.publish(publish_parameters=publish_params, overwrite=overwrite)
    item.share(everyone=True)


def main():
    data = fetch()

    keys = data[0].keys()
    with open(TEMP_PATH, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

    upload_arcgis()


if __name__ == "__main__":
    main()
