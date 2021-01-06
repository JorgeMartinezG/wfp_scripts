import csv
import logging
import requests
import os

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    Boolean,
    ForeignKey,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Geometry, func
from sqlalchemy.exc import ProgrammingError

from datetime import date
from multiprocessing import Pool

load_dotenv("config.env")

API_URL = os.getenv("API_URL")
DB_URL = os.getenv("DB_URL")
DB_SCHEMA = os.getenv("DB_SCHEMA")

engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


class Market(Base):
    __tablename__ = "wld_markets"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True)
    code = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    geom = Column(Geometry("POINT"))
    created_at = Column(Date, nullable=False)
    last_updated = Column(Date)
    deleted = Column(Boolean)


class MarketHistory(Base):
    __tablename__ = "wld_markets_history"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True)
    market_id = Column(
        Integer, ForeignKey(Market.id, name="fk_markets"), nullable=False
    )
    action_date = Column(Date, nullable=False)
    action = Column(String, nullable=False)


def get_token():
    auth = (os.getenv("CONSUMER_KEY"), os.getenv("CONSUMER_SECRET"))
    data = dict(grant_type="client_credentials")

    token_url = f"{API_URL}/token"
    logging.info("Request for token")

    resp = requests.post(token_url, data=data, auth=auth)
    if resp.status_code != 200:
        raise ValueError("Invalid token response")
    response_json = resp.json()
    if "access_token" not in response_json.keys():
        raise ValueError("Token not provided in response")

    return response_json.get("access_token")


def get_markets(params):
    token, country = params
    logging.info(f"Fetching markets for country {country['name']}")

    url = f"{API_URL}/vam-data-bridges/1.0.0/Markets/GeoJSONList"

    request_params = {
        "url": url,
        "headers": {"Authorization": f"Bearer {token}"},
        "params": {"adm0code": country["code"]},
    }

    resp = requests.get(**request_params)
    if resp.status_code != 200:
        return None

    data = resp.json()

    # Parsing objects.
    markets = []
    for market_dict in data.get("features"):
        x, y = market_dict.get("geometry").get("coordinates")
        name = market_dict.get("properties").get("name")
        id = market_dict.get("id")

        market = Market(
            id=id,
            name=name,
            code=country["code"],
            geom=f"POINT ({x} {y})",
            created_at=RUN_DATE,
        )
        markets.append(market)

    return markets


def main():
    try:
        engine.execute(CreateSchema(DB_SCHEMA))
    except ProgrammingError:
        pass

    # Create tables.
    Base.metadata.create_all(engine)

    token = get_token()

    # Read csv file
    with open(os.getenv("ADMIN0_FILE"), "r") as f:
        reader = csv.DictReader(f)
        codes = [(token, r) for r in reader][:4]

    with Pool() as p:
        api_data = p.map(get_markets, codes)

    api_data = [item for sublist in api_data for item in sublist]

    add_markets = []
    markets_history = []
    for market in api_data:
        # Check if elements are equal..if exists in db.
        res = (
            session.query(
                Market,
                func.st_equals(Market.geom, market.geom),
                (Market.name == market.name),
            )
            .filter(Market.id == market.id)
            .one_or_none()
        )

        if res is None:
            add_markets.append(market)
            continue

        market_db, geom_equal, name_equal = res
        update = False
        if geom_equal is False:
            history = MarketHistory(
                action="UPDATED_GEOM",
                market_id=market_db.id,
                action_date=RUN_DATE,
            )
            markets_history.append(history)

            market_db.geom = market.geom
            update = True

        if name_equal is False:
            history = MarketHistory(
                action="UPDATED_NAME",
                market_id=market_db.id,
                action_date=RUN_DATE,
            )
            markets_history.append(history)

            market_db.name = market.name
            update = True

        if update is True:
            market_db.last_updated = RUN_DATE
            session.add(market_db)

    len_add_markets = len(add_markets)
    if len_add_markets > 0:
        session.add_all(add_markets)
        session.flush()

        # add_markets_history = [
        #     MarketHistory(
        #         action="CREATE", market_id=m.id, action_date=RUN_DATE
        #     )
        #     for m in add_markets
        # ]
        # markets_history.extend(add_markets_history)

        logging.info(f"Added {len_add_markets} markets into the database")

    session.add_all(markets_history)
    logging.info(f"Updated {len(markets_history)} markets")

    # Check elements that are needed to be removed.
    markets = session.query(Market).all()
    api_ids = [m.id for m in api_data]

    markets_delete = [m for m in markets if m.id not in api_ids]
    for market in markets_delete:
        market.deleted = True
        session.add(market)

    session.commit()


if __name__ == "__main__":
    RUN_DATE = date.today()
    main()
