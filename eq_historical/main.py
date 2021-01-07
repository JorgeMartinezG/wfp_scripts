import logging
import requests
import optparse
import os

from dotenv import load_dotenv
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

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

load_dotenv("config.env")

API_URL = os.getenv("API_URL")
DB_URL = os.getenv("DB_URL")
DB_SCHEMA = os.getenv("DB_SCHEMA")

engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
session = Session()

DATE_RUN = date.today()

Base = declarative_base()


class Earthquake(Base):
    __tablename__ = "wld_eq_historical"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(String, primary_key=True)
    geom = Column(Geometry("POINT"))
    mag = (Column(Float),)
    place = Column(String)
    time = Column(DateTime, nullable=False)
    url = Column(String)
    tsunami = Column(Integer)
    mmi = Column(Integer)
    sig = Column(Integer)
    title = Column(String, nullable=False)
    ids = Column(String)
    sources = Column(String)


def to_obj(feature, db_ids):
    obj_id = feature.get("id")

    if obj_id in db_ids:
        logging.info(f"Skipping object {obj_id}")
        return None

    fields = [
        "mag",
        "place",
        "time",
        "url",
        "tsunami",
        "mmi",
        "sig",
        "title",
        "ids",
        "sources",
    ]

    item = {"id": obj_id}

    # Parse parameters.
    for k, v in feature.get("properties").items():
        if k not in fields:
            continue

        value = v
        if k == "time":
            value = datetime.fromtimestamp(v / 1000.0)  # milliseconds.

        item[k] = value

    # Create geom.
    geom = feature.get("geometry")

    feat_type = geom.get("type")
    coords = " ".join([str(c) for c in geom.get("coordinates")[:2]])

    item["geom"] = f"{feat_type} ({coords})"

    obj = Earthquake(**item)

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

    resp = requests.get(API_URL, params=params)
    if resp.status_code != 200:
        raise ValueError("could not fetch data from server.")

    data = resp.json()

    response_ids = [r.get("id") for r in data.get("features")]

    # Get all ids which are in the database already.
    db_ids = session.query(Earthquake.id).filter(Earthquake.id.in_(response_ids)).all()
    db_ids = [r.id for r in db_ids]

    objs = [to_obj(f, db_ids) for f in data.get("features")]

    session.add_all([o for o in objs if o is not None])
    session.commit()


def fetch_all():
    step = 5  # years

    start_date = date(1900, 1, 1)
    while start_date < DATE_RUN:
        end_date = min(start_date + timedelta(days=365 * step), DATE_RUN)

        request_api(start_date, end_date)

        start_date = end_date + timedelta(days=1)


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "-w", "--weekly", action="store_true", dest="weekly", default=False
    )
    options, _ = parser.parse_args()

    # Database check.
    try:
        engine.execute(CreateSchema(DB_SCHEMA))
    except ProgrammingError:
        pass

    # Create tables.
    Base.metadata.create_all(engine)

    if options.weekly == False:
        fetch_all()


if __name__ == "__main__":
    main()
