import csv
import os
import requests

from multiprocessing import Pool


def get_data(item):
    print(f"Processing city {item['city_name']}")

    params = {
        "addressdetails": 1,
        "accept-language": "en",
        "namedetails": 1,
        "format": "json",
        "lat": item["lat"],
        "lon": item["lng"],
    }
    resp = requests.get(NOMINATIM_URL, params=params)

    resp_json = resp.json()

    address = resp_json["address"]

    out = {"objectid": item["objectid"], "city_name": item["city_name"]}

    for field in ADDRESS_FIELDS:
        out[field] = address.get(field, None)

    return address.keys()


def main():
    with open(DUMP_FILE, "r") as f:
        reader = csv.DictReader(f)

        data = [r for r in reader]

    with Pool() as p:
        addresses = p.map(get_data, data)

    keys = addresses[0].keys()
    with open(OUT_FILE, "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(addresses)


if __name__ == "__main__":
    ADDRESS_FIELDS = [
        "village",
        "neighbourhood",
        "city",
        "municipality",
        "county",
        "state_district",
        "postcode",
        "state",
        "region",
        "island",
        "archipelago",
        "country",
        "country_code",
        "continent",
    ]

    NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

    DUMP_FILE = os.getenv("DUMP_FILE", "dump.csv")
    OUT_FILE = os.getenv("OUT_FILE", "out.csv")
    main()
