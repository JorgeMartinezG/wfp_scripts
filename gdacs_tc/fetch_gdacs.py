import requests
from enum import Enum
from os.path import basename
from urllib.parse import urljoin
from lxml import html

from dateutil import parser as dateparser

GDACS_URL = "https://www.gdacs.org"


class EventType(Enum):
    TC = "TC"
    EQ = "EQ"


def list_events_paths(event_type):
    events_list_url = urljoin(GDACS_URL, f"datareport/resources/{event_type}")
    resp = requests.get(events_list_url)

    # Parse html to lxml object.
    tree = html.fromstring(resp.content.decode("utf-8"))
    links = tree.body.find("pre").findall("a")
    events_urls = [
        link.attrib.get("href")
        for link in links
        if event_type in link.attrib.get("href")
    ]

    return events_urls


def list_ordered_geojsons(path):
    resp = requests.get(urljoin(GDACS_URL, path))
    tree = html.fromstring(resp.content.decode("utf-8"))
    links = tree.body.find("pre").findall("a")

    links = [
        link.attrib.get("href")
        for link in links
        if link.attrib.get("href").endswith(".geojson")
    ]

    links = [l for l in links if len(l.split("_")) == 3]

    links.sort(
        key=lambda f: int(basename(f).split("_")[-1].split(".")[0]),
        reverse=True,
    )

    return links


def download_geojson_as_feature(path):
    feature_collection = requests.get(urljoin(GDACS_URL, path)).json()
    features = feature_collection.get("features")

    return features


def filter_features(features, geometry_type):
    return [
        f for f in features if f.get("geometry").get("type") == geometry_type
    ]


def process_event(event_path):
    print(f"Processing event {event_path}")
    geojsons_paths = list_ordered_geojsons(event_path)

    paths_iter = iter(geojsons_paths)
    latest = next(paths_iter)

    features = download_geojson_as_feature(latest)

    geometry_types = ["Polygon", "LineString", "Point"]
    geometries = {
        k.lower(): filter_features(features, k) for k in geometry_types
    }

    # Get additional points for previous episodes.
    if len(geometries.get("point")) == 1:
        for path in paths_iter:
            features = download_geojson_as_feature(path)
            point = next(
                iter(filter_features(features, "Point")),
                None,
            )
            if point is None:
                continue
            point["path"] = path
            geometries.get("point").append(point)

    points_dict = []
    for point in geometries.get("point"):
        coords = point.get("geometry").get("coordinates")
        props = point.get("properties")

        # Validate dates.
        datetime_keys = [k for k in props.keys() if "date" in k]
        datetime_keys.sort(reverse=True)

        try:
            datetime_value = dateparser.parse(props.get(datetime_keys[0]))
        except:
            print("FAILLLLLL:", point, event_path)
            continue

        fields = {
            "timestamp": datetime_value,
            "event_id": props.get("eventid"),
            "event_name": props.get("eventname"),
            "wind_speed": float(props.get("windspeed", 0.0)),
            "shape": coords,
        }

        points_dict.append(fields)

    if len(points_dict) == 0:
        return

    points_dict.sort(key=lambda x: x.get("timestamp"), reverse=True)

    # Update points.
    fields = {
        k: v
        for k, v in points_dict[0].items()
        if k in ["timestamp", "event_id", "event_name"]
    }
    processed_points = [
        {**fields, "released_date": p.get("timestamp")} for p in points_dict
    ]

    #print([d.get("released_date").isoformat() for d in processed_points])
    #return processed_points


event_type = EventType("TC")
events_paths = list_events_paths(event_type.name)

from multiprocessing import Pool

with Pool() as p:
    p.map(process_event, events_paths)

