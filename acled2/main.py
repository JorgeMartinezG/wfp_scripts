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
TEMP_PATH = f"/tmp/{FILENAME}"


def fetch(all):
    ACLED_API_URL = config.get("ACLED", "API_URL")
    ACLED_KEY = config.get("ACLED", "KEY")
    ACLED_EMAIL = config.get("ACLED", "EMAIL")
    ISO_COUNTRIES = config.get("ACLED", "ISO_COUNTRIES").split(",")
    START_DATE = config.get("MISC", "START_DATE")

    logger.info("Fetching data from acled API")

    date_today = date.today().isoformat()

    params = dict(
        key=ACLED_KEY,
        email=ACLED_EMAIL,
        iso="|".join(ISO_COUNTRIES),
        page=1,
        event_date=date_today,
    )

    if all is True:
        params.update(
            dict(
                event_date=f"{START_DATE}|{date_today}",
                event_date_where="BETWEEN",
            )
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


def upload_arcgis(gis, item, path):
    publish_params = dict(
        name=FILENAME,
        type="csv",
        locationType="coordinates",
        latitudeFieldName="latitude",
        longitudeFieldName="longitude",
    )

    if item is not None:
        logger.info("Appending items to layer")
        lyr = item.layers[0]
        temp_item = gis.content.add(dict(title=f"{FILENAME}_temp"), data=path)

        lyr.append(
            item_id=temp_item.id,
            upload_format="csv",
            upsert=False,
            source_info=publish_params,
        )

        # Delete old item.
        temp_item.delete()

        return

    logger.info("Uploading layer")
    item = gis.content.add(dict(title=FILENAME), data=path)

    item = item.publish(publish_parameters=publish_params)
    item.share(everyone=True)


def main():
    ARCGIS_USER = config.get("ARCGIS", "USER")
    ARCGIS_PW = config.get("ARCGIS", "PW")
    ARCGIS_URL = config.get("ARCGIS", "URL")

    gis = GIS(ARCGIS_URL, ARCGIS_USER, ARCGIS_PW)
    content_data = f"title:{FILENAME} type:Feature Service owner:{ARCGIS_USER}"
    item = next((f for f in gis.content.search(content_data)), None)

    all = False
    if item is None:
        all = True

    data = fetch(all)

    path = TEMP_PATH
    if item is not None:
        path += "_temp"
    path += ".csv"

    if len(data) == 0:
        logger.info("Data not found")
        return

    keys = data[0].keys()
    with open(path, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

    upload_arcgis(gis, item, path)


if __name__ == "__main__":
    main()
