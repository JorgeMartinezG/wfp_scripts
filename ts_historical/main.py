import logging
import requests
from datetime import datetime

import optparse
import os

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Geometry
from sqlalchemy.exc import ProgrammingError

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

load_dotenv("config.env")

engine = create_engine(os.getenv("DB_URL"))
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class Ibtracs(Base):
    __tablename__ = os.getenv("TABLE_NAME")
    __table_args__ = {"schema": os.getenv("DB_SCHEMA")}

    id = Column(String, primary_key=True)
    sid = Column(String, nullable=False)
    season = Column(String, nullable=False)
    number = Column(Integer, nullable=False)
    basin = Column(String, nullable=False)
    subbasin = Column(String, nullable=False)
    name = Column(String, nullable=False)
    iso_time = Column(DateTime, nullable=False)
    nature = Column(String, nullable=False)
    dist2land = Column(Integer)
    landfall = Column(Integer)
    storm_speed = Column(Integer)
    storm_dir = Column(Integer)
    shape = Column(Geometry(geometry_type="POINT", srid=4326))


REQUIRED_FIELDS = {
    "sid": 0,
    "season": 1,
    "number": 2,
    "basin": 3,
    "subbasin": 4,
    "name": 5,
    "iso_time": 6,
    "nature": 7,
    "lat": 8,
    "lon": 9,
    "dist2land": 14,
    "landfall": 15,
    "storm_speed": 161,
    "storm_dir": 162,
}


def parse(item):
    dict_item = {}
    for key, value in REQUIRED_FIELDS.items():
        parsed_value = item[value]

        if key in ["lat", "lon"]:
            parsed_value = float(parsed_value)

        if key == "iso_time":
            parsed_value = datetime.strptime(parsed_value, "%Y-%m-%d %H:%M:%S")

        if key in ["dist2land", "landfall", "storm_speed", "storm_dir"]:
            try:
                parsed_value = int(parsed_value)
            except ValueError:
                parsed_value = None

        dict_item[key] = parsed_value

    obj = {k: v for k, v in dict_item.items() if k not in ["lat", "lon"]}

    # Set geometry column.
    lon, lat = dict_item["lon"], dict_item["lat"]
    obj["shape"] = f"SRID=4326; POINT ({lon} {lat})"

    # Set unique identifier.
    obj_id = "{sid}/{timestamp}".format(
        sid=dict_item["sid"], timestamp=int(dict_item["iso_time"].timestamp())
    )
    obj["id"] = obj_id

    obj = Ibtracs(**obj)

    return obj


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "-a", "--all", action="store_true", dest="all", default=False
    )
    options, _ = parser.parse_args()
    # Database check.
    try:
        engine.execute(CreateSchema(os.getenv("DB_SCHEMA")))
    except ProgrammingError:
        pass

    # Create tables.
    Base.metadata.create_all(engine)

    url_field = "API_URL"
    if options.all is True:
        url_field = "ALL_API_URL"

    file_name = os.getenv(url_field)
    logging.info(f"Using file name {file_name}")
    resp = requests.post(file_name)
    if resp.status_code != 200:
        raise ValueError("Could not download data")

    data = [d.split(",") for d in resp.content.decode("utf-8").splitlines()]

    # Parse to dict.
    data = [parse(d) for d in data[2:]]

    search_ids = (
        session.query(Ibtracs.id)
        .filter(Ibtracs.id.in_([d.id for d in data]))
        .all()
    )
    search_ids = [s.id for s in search_ids]

    # Filter already created.
    data = [d for d in data if d.id not in search_ids]

    logging.info(f"Skipping ids {search_ids}")

    session.add_all(data)
    session.commit()


if __name__ == "__main__":
    main()
