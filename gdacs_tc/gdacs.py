import logging
import requests
from lxml import html, etree
from shapely.geometry import shape, LineString
from shapely.wkt import loads
from os.path import basename
from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column,
    Float,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from optparse import OptionParser

from dateutil import parser as dateparser

from configparser import ConfigParser

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

parser = OptionParser()
parser.add_option("-r", "--rss", dest="rss", action="store_true", default=False)
parser.add_option("-c", "--config", dest="config", default="config.txt")

options, _ = parser.parse_args()

if options.config is None:
    raise ValueError("Required configuration file")

# get config info
config = ConfigParser()
config.read(options.config)

GDACS_URL = config.get("GDACS", "URL")

DB_HOST = config.get("PG", "HOST")
DB_USER = config.get("PG", "USER")
DB_PW = config.get("PG", "PW")
DB_NAME = config.get("PG", "NAME")
DB_SCHEMA = config.get("PG", "SCHEMA")
DB_PORT = config.get("PG", "PORT")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

REQUIRED_POLYGON_CLASSES = [
    "Poly_Green",
    "Poly_Orange",
    "Poly_Red",
    "Poly_Cones",
]


class Node(Base):
    __tablename__ = "wld_gdacs_tc_events_nodes"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer)
    episode_id = Column(String)
    event_name = Column(String)
    wind_speed = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    released_date = Column(DateTime)
    shape = Column(Geometry("POINT", 4326))


class Buffer(Base):
    __tablename__ = "wld_gdacs_tc_events_buffers"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer)
    episode_id = Column(String)
    event_name = Column(String)
    timestamp = Column(DateTime, nullable=False)
    label = Column(String, nullable=False)
    shape = Column(Geometry("POLYGON", 4326))


class Track(Base):
    __tablename__ = "wld_gdacs_tc_events_tracks"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer)
    episode_id = Column(String)
    event_name = Column(String)
    timestamp = Column(DateTime, nullable=False)
    shape = Column(Geometry("LINESTRING", 4326))


def get_html_response(url):
    resp = requests.get(url)
    tree = html.fromstring(resp.content)

    return tree


def get_tc_paths(dataset_url):
    tree = get_html_response(dataset_url)

    links = tree.body.find("pre").findall("a")
    links = [
        link.attrib.get("href")
        for link in links
        if "TC" in link.attrib.get("href")
    ]

    return links


def get_file_number(file_name):
    return int(basename(file_name).split("_")[-1].split(".")[0])


def get_geojsons_paths(tc_url):
    tree = get_html_response(f"{GDACS_URL}{tc_url}")
    links = tree.body.find("pre").findall("a")
    links = [
        link.attrib.get("href")
        for link in links
        if "geojson" in link.attrib.get("href")
    ]

    links = [link for link in links if len(link.split("_")) == 3]

    links.sort(key=lambda x: get_file_number(x))

    return links


def get_datetime(props):
    date_time = props.get("todate")

    if date_time is None:
        date_time = props.get("jrc_pubdate")

    if date_time is None:
        date_time = props.get("fromdate")

    if date_time is None:
        date_time = props.get("trackdate")

    return dateparser.parse(date_time)


def get_points(features):
    return [f for f in features if f.get("geometry").get("type") == "Point"]


def get_nodes_and_fields(json_features):
    points = get_points(json_features)

    fields = None
    nodes = []

    init_props = points[0].get("properties")

    fields = {
        "event_id": init_props.get("eventid"),
        "event_name": init_props.get("eventname"),
        "episode_id": init_props.get("episodeid"),
        "timestamp": get_datetime(init_props),
    }

    for point in points:
        props = point.get("properties")
        geom = shape(point.get("geometry")).wkt

        fields_copy = fields.copy()

        try:
            released_date = get_datetime(props)
        except:
            released_date = fields.get("timestamp")

        fields_copy.update(
            {
                "shape": f"SRID=4326; {geom}",
                "released_date": released_date,
                "wind_speed": float(props.get("windspeed", 0.0)),
            }
        )

        node = Node(**fields_copy)
        nodes.append(node)
    return nodes, fields


def get_buffers(json_features, fields):
    buffers = []
    features = [
        f
        for f in json_features
        if f.get("geometry").get("type") == "Polygon"
        and f.get("properties").get("Class") in REQUIRED_POLYGON_CLASSES
    ]

    for feature in features:
        geom = shape(feature.get("geometry")).wkt

        fields_copy = fields.copy()

        fields_copy.update(
            {
                "shape": f"SRID=4326; {geom}",
                "label": feature.get("properties").get("polygonlabel"),
            }
        )

        buff = Buffer(**fields_copy)
        buffers.append(buff)

    return buffers


def get_tracks(json_features, fields):
    features = [
        f
        for f in json_features
        if f.get("geometry").get("type") == "LineString"
    ]

    if len(features) == 0:
        return []

    tracks = []

    for feat in features:
        geom = shape(feat.get("geometry")).wkt

        fields_copy = fields.copy()
        fields_copy["shape"] = f"SRID=4326; {geom}"

        track = Track(**fields_copy)
        tracks.append(track)

    return tracks


def get_missing_nodes(paths, fields):

    missing_nodes = []
    for event in paths:
        resp = requests.get(f"{GDACS_URL}{event}").json()
        points = get_points(resp.get("features"))

        for point in points:
            geom = shape(point.get("geometry")).wkt
            fields_copy = fields.copy()
            fields_copy["shape"] = f"SRID=4326; {geom}"
            fields_copy["wind_speed"] = float(
                point.get("properties").get("windspeed", 0.0)
            )

            node = Node(**fields_copy)
            missing_nodes.append(node)

    return missing_nodes


def create_track(nodes, fields):
    wkt_points = [loads(n.shape.split(";")[1].strip()) for n in nodes]
    geom = LineString(wkt_points).wkt
    # geom = MultiLineString([track_linestring]).wkt

    fields_copy = fields.copy()
    fields_copy["shape"] = f"SRID=4326; {geom}"

    track = Track(**fields_copy)

    return [track]


def process_tc_par(path):
    try:
        process_tc(path)
    except Exception as e:
        logging.error(f"Failed processing path {path}")
        logging.error(e)


def process_tc(path):
    tc_event_id = path.split("/")[-2]
    logging.error(f"Processing event: {tc_event_id}")

    ordered_paths = get_geojsons_paths(path)

    # Get last result.
    latest = requests.get(f"{GDACS_URL}{ordered_paths[-1]}").json()

    features = latest.get("features")
    if len(features) == 0:
        logging.warning(f"Event without features: {tc_event_id}")
        return

    nodes, fields = get_nodes_and_fields(features)
    missing_nodes = []
    if len(nodes) == 1:
        logging.info(f"Collecting points from previous reports: {tc_event_id}")
        missing_nodes = get_missing_nodes(ordered_paths[:-1], fields)

    nodes = missing_nodes + nodes

    # TC event still has a single point, discard it.
    if len(nodes) == 1:
        logging.warning(f"Discarding event {tc_event_id}")
        return

    buffers = get_buffers(features, fields)
    tracks = get_tracks(features, fields)

    # Create Linestring from list of points.
    if len(tracks) == 0:
        logging.info(f"Creating track from points: {tc_event_id}")
        tracks = create_track(nodes, fields)
    rows = nodes + tracks + buffers

    logging.info(f"Save into database: {tc_event_id}")

    session.add_all(rows)
    session.commit()


def get_events_from_rss():
    rss_url = f"{GDACS_URL}/xml/rss.xml"

    resp = requests.get(rss_url)

    xml_obj = etree.fromstring(resp.content)
    channel = xml_obj.getchildren()[0]
    items = [c for c in channel.getchildren() if c.tag == "item"]

    tc_events = []
    for item in items:
        event_type = item.find("gdacs:eventtype", item.nsmap).text

        if event_type != "TC":
            continue

        event_id = item.find("gdacs:eventid", item.nsmap).text
        episode_id = item.find("gdacs:episodeid", item.nsmap).text

        tc_events.append((event_id, episode_id))

    return tc_events


def process_rss_event(event):
    # Download file.
    event_id, episode_id = event

    query = session.query(Node).filter(Node.event_id == event_id)
    num_rows = query.filter(Node.episode_id == episode_id).count()

    # If the episode is already in the db, do nothing.
    if num_rows > 0:
        logging.info(
            f"Episode {episode_id} for event {event_id} already within the db"
        )
        return

    event_url = f"{GDACS_URL}/datareport/resources/TC/{event_id}/geojson_{event_id}_{episode_id}.geojson"
    print(event_url)

    resp = requests.get(event_url).json()

    features = resp.get("features")
    session.query(Node).filter(Node.event_id == event_id).delete()

    nodes, fields = get_nodes_and_fields(features)
    session.add_all(nodes)
    session.commit()

    all_nodes = session.query(Node).filter(Node.event_id == event_id).all()
    # Transform to wkt.
    for node in all_nodes:
        node.shape = f"SRID=4326; {to_shape(node.shape).wkt}"

    track = create_track(all_nodes, fields)
    buffers = get_buffers(features, fields)

    # Delete previous track and buffer.
    session.query(Track).filter(Track.event_id == event_id).delete()

    session.query(Buffer).filter(Buffer.event_id == event_id).delete()

    session.add_all(track + buffers)

    session.commit()


def update_database(events):
    for event in events:
        process_rss_event(event)


def main():
    # Database check.
    try:
        engine.execute(CreateSchema(DB_SCHEMA))
    except ProgrammingError:
        pass

    # Create tables.
    Base.metadata.create_all(engine)

    if options.rss is True:
        tc_events = get_events_from_rss()
        update_database(tc_events)
        return

    dataset_url = f"{GDACS_URL}/datareport/resources/TC"
    tc_paths = get_tc_paths(dataset_url)

    for idx, path in enumerate(tc_paths):
        try:
            process_tc(path)
        except Exception as e:
            logging.error(f"Failed processing path {path}")
            logging.error(e)


if __name__ == "__main__":
    """
    process_tc("/datareport/resources/TC/1000268/")
    exit()
    """

    main()
