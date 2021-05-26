import logging
import requests
from configparser import ConfigParser
from os.path import join
from arcgis.features import FeatureLayerCollection
from arcgis.gis import GIS

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-c", "--config", dest="config", default="config.txt")
options, _ = parser.parse_args()

logger = logging.getLogger()
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

def upload_arcgis():
    ARCGIS_USER = config.get("ARCGIS", "USER")
    ARCGIS_PW = config.get("ARCGIS", "PW")
    ARCGIS_URL = config.get("ARCGIS", "URL")

    gis = GIS(ARCGIS_URL, ARCGIS_USER, ARCGIS_PW)
    content_data = f"title:{FILENAME} type:CSV owner:{ARCGIS_USER}"

    items = gis.content.search(content_data)

    item_params = dict(title=FILENAME)
    if len(items) > 0:
        logger.info("Overwriting layer")
        items = gis.content.search(f"type: Feature Service owner:{ARCGIS_USER}")
        feature_layer_item = next((n for n in items if n.title == FILENAME))

        feature_layer = FeatureLayerCollection.fromitem(feature_layer_item)
        feature_layer.manager.overwrite(TEMP_PATH)

        return

    logger.info("Uploading file to Arcgis")
    item = gis.content.add(item_params, data=TEMP_PATH)

    item.share(everyone=True)

    logger.info("Publishing layer")
    publish_params = dict(
        name=FILENAME,
        type="csv",
        locationType="coordinates",
        latitudeFieldName="Y",
        longitudeFieldName="X",
    )

    item = item.publish(publish_parameters=publish_params, overwrite=overwrite)
    item.share(everyone=True)


def main():
    logger.info("Downloading data")
    resp = requests.get(config.get("MISC", "GOOGLE_URL"))
    with open(TEMP_PATH, "wb") as f:
        for chunk in resp.iter_content():
            f.write(chunk)

    upload_arcgis()


if __name__ == "__main__":
    config = ConfigParser()
    config.read(options.config)

    FILENAME = config.get("MISC", "FILENAME")
    PATH = config.get("MISC", "PATH")

    TEMP_PATH = join(PATH, f"{FILENAME}.csv")

    main()

    logger.info("Done")
