import logging
import requests
import optparse

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Geometry
from sqlalchemy.exc import ProgrammingError

from datetime import date, datetime, timedelta

from configparser import ConfigParser

from zipfile import ZipFile

from osgeo import ogr

from os.path import join

from tempfile import TemporaryDirectory

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

parser = optparse.OptionParser()
parser.add_option(
    "-r", "--rss", action="store_true", dest="rss", default=False
)
parser.add_option("-c", "--config", dest="config", default="config.txt")
options, _ = parser.parse_args()

config = ConfigParser()
config.read(options.config)

DB_SCHEMA = config.get("DB", "SCHEMA")

DATE_RUN = date.today()

Base = declarative_base()

TABLE_COLUMNS = ["mag", "place", "time", "mmi", "title", "id"]

engine = create_engine(config.get("DB", "URL"))
Session = sessionmaker(bind=engine)

from multiprocessing import Pool


class Earthquake(Base):
    __tablename__ = config.get("DB", "EVENTS_TABLE_NAME")
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(String, primary_key=True)
    shape = Column(Geometry(geometry_type="POINT", srid=4326))
    mag = Column(Float)
    place = Column(String)
    time = Column(DateTime, nullable=False)
    mmi = Column(Integer)
    title = Column(String, nullable=False)


class ShakeMap(Base):
    __tablename__ = config.get("DB", "SM_TABLE_NAME")
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True)
    shape = Column(Geometry(geometry_type="POLYGON", srid=4326))
    mmi = Column(Float, nullable=False)
    eq_id = Column(String, nullable=False)
    time = Column(DateTime, nullable=False)


def download_shakemap_polygons(detail_url, item):
    resp = requests.get(detail_url)
    resp.raise_for_status()

    zip_url = (
        resp.json()
        .get("properties")
        .get("products")
        .get("shakemap")[0]
        .get("contents")
        .get("download/shape.zip")
        .get("url")
    )

    temp_folder = TemporaryDirectory()

    FILE_PATH = join(temp_folder.name, "{}.zip".format(item.get("id")))

    with requests.get(zip_url, stream=True) as r:
        r.raise_for_status()
        with open(FILE_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    with ZipFile(FILE_PATH, "r") as zip_file:
        zip_file.extractall(temp_folder.name)

    driver = ogr.GetDriverByName("ESRI Shapefile")
    shp = driver.Open(join(temp_folder.name, "mi.shp"))
    layer = shp.GetLayer()

    sql_objects = []
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)

        mmi = feature.GetField("PARAMVALUE")

        geom = feature.GetGeometryRef()
        geom_name = geom.GetGeometryName()

        if geom_name not in ("MULTIPOLYGON", "POLYGON"):
            logging.warn(
                f"Earthquake {item.get('id')} contains geometry not supported: {geom_name}"
            )
            continue

        # Split multipolygon into array of polygons.
        geom_array = [geom] if geom_name == "POLYGON" else [g for g in geom]

        for geom in geom_array:
            sql_objects.append(
                ShakeMap(
                    eq_id=item.get("id"),
                    mmi=mmi,
                    shape=f"SRID=4326;{geom.ExportToWkt()}",
                    time=item.get("time"),
                )
            )

    session = Session()
    session.add_all(sql_objects)
    session.commit()

    temp_folder.cleanup()


def parse_feature(feature):
    properties = feature.get("properties")

    # Parse parameters.
    item = {"id": feature.get("id")}
    for k, v in properties.items():
        if k not in TABLE_COLUMNS:
            continue

        value = v
        if k == "time":
            value = datetime.fromtimestamp(v / 1000.0).date()  # milliseconds.

        item[k] = value

    # Create geom.
    geom = feature.get("geometry")

    feat_type = geom.get("type")
    coords = " ".join([str(c) for c in geom.get("coordinates")[:2]])

    item["shape"] = f"SRID=4326;{feat_type} ({coords})"

    obj = Earthquake(**item)

    if "shakemap" in properties.get("types"):
        logging.info("Collect shakemap data for id: {}".format(item.get("id")))
        detail_url = properties.get("detail")
        download_shakemap_polygons(detail_url, item)

    return obj


def request_api(start_date, end_date):
    start_time = start_date.isoformat()
    end_time = end_date.isoformat()

    logging.info(f"Fetching earthquakes: {start_time} - {end_time}")

    params = dict(
        starttime=start_time,
        endtime=end_time,
        minmagnitude=5,
        format="geojson",
    )

    resp = requests.get(config.get("USGS", "API_URL"), params=params)
    if resp.status_code != 200:
        raise ValueError("could not fetch data from server.")

    data = resp.json()
    features = data.get("features")

    response_ids = [r.get("id") for r in features]

    session = Session()

    with Pool() as p:
        p.map(parse_feature, features)

    '''
    session.add_all([o for o in objs if o is not None])
    session.commit()
    '''


def fetch_all():
    step = 5  # years
    start_date = date(2010, 1, 1)
    while start_date < DATE_RUN:
        end_date = min(start_date + timedelta(days=365 * step), DATE_RUN)
        request_api(start_date, end_date)
        start_date = end_date


def fetch_most_recent():
    logging.info("Fetching last two weeks report")
    start_date = DATE_RUN - timedelta(days=15)
    request_api(start_date, DATE_RUN)


def main():
    # Database check.
    try:
        engine.execute(CreateSchema(DB_SCHEMA))
    except ProgrammingError:
        pass

    # Create tables.
    Base.metadata.create_all(engine)

    fetch_all()


if __name__ == "__main__":
    main()
