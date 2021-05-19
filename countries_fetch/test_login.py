#! /usr/bin/env python3
import json
import re
import os
import requests
import sys

from os import remove
from os.path import join
from osgeo import gdal

gdal.SetConfigOption("OGR_INTERLEAVED_READING", "YES")    
os.environ["OSM_CONFIG_FILE"] = "./customconf.ini"

CUSTOM_HEADER = {"user-agent": "oauth_cookie_client.py"}
OSM_HOST = "https://www.openstreetmap.org/"
PATH = "/tmp"


def find_authenticity_token(response):
    pattern = r"name=\"csrf-token\" content=\"([^\"]+)\""
    m = re.search(pattern, response)
    if m is None:
        raise ValueError(
            "Could not find the authenticity_token in the website to be scraped."
        )
    try:
        return m.group(1)
    except IndexError:
        sys.stderr.write(
            "ERROR: The login form does not contain an authenticity_token.\n"
        )
        exit(1)


def get_tokens(consumer_url):
    url = f"{consumer_url}?action=request_token"
    resp = requests.post(url, data={}, headers=CUSTOM_HEADER)
    if resp.status_code != 200:
        raise ValueError(
            "POST {}, received HTTP status code {} but expected 200".format(
                url, resp.status_code
            )
        )

    json_response = resp.json()

    oauth_token = json_response.get("oauth_token")
    oauth_token_secret_encr = json_response.get("oauth_token_secret_encr")

    return oauth_token, oauth_token_secret_encr


def create_osm_session(username, password):
    login_url = f"{OSM_HOST}/login?cookie_test=true"
    session = requests.Session()
    resp = session.get(login_url, headers=CUSTOM_HEADER)
    if resp.status_code != 200:
        raise ValueError(
            "GET {}, received HTTP code {}".format(login_url, resp.status_code)
        )

    # login
    authenticity_token = find_authenticity_token(resp.text)
    resp = session.post(
        f"{OSM_HOST}/login",
        data={
            "username": username,
            "password": password,
            "referer": "/",
            "commit": "Login",
            "authenticity_token": authenticity_token,
        },
        allow_redirects=False,
        headers=CUSTOM_HEADER,
    )
    if resp.status_code != 302:
        raise ValueError(
            "POST {}, received HTTP code {} but expected 302".format(
                login_url, resp.status_code
            )
        )

    return session


def authorize(session, oauth_token):
    authorize_url = f"{OSM_HOST}/oauth/authorize?oauth_token={oauth_token}"
    resp = session.get(authorize_url, headers=CUSTOM_HEADER)
    if resp.status_code != 200:
        raise ValueError(
            "GET {}, received HTTP code {} but expected 200".format(
                authorize_url, resp.status_code
            )
        )
    authenticity_token = find_authenticity_token(resp.text)

    post_data = {
        "oauth_token": oauth_token,
        "oauth_callback": "",
        "authenticity_token": authenticity_token,
        "allow_read_prefs": "yes",
        "commit": "Save changes",
    }
    authorize_url = f"{OSM_HOST}/oauth/authorize"
    resp = session.post(authorize_url, data=post_data, headers=CUSTOM_HEADER)
    if resp.status_code != 200:
        raise ValueError(
            "POST {}, received HTTP code {} but expected 200".format(
                authorize_url, resp.status_code
            )
        )


def logout(consumer_url, session, oauth_token, oauth_token_secret_encr):
    # logout
    logout_url = f"{OSM_HOST}/logout"
    resp = session.get(logout_url, headers=CUSTOM_HEADER)
    if resp.status_code != 200 and resp.status_code != 302:
        raise ValueError(
            "POST {}, received HTTP code {} but expected 200 or 302".format(
                logout_url, resp.status_code
            )
        )

    # get final cookie
    resp = requests.post(
        f"{consumer_url}?action=get_access_token_cookie&format=http",
        data={
            "oauth_token": oauth_token,
            "oauth_token_secret_encr": oauth_token_secret_encr,
        },
        headers=CUSTOM_HEADER,
    )

    cookie_text = resp.text
    if not cookie_text.endswith("\n"):
        cookie_text += "\n"

    return cookie_text


def process_country(values, cookies):

    schema = "_".join(values.get("iso2")).lower()
    download_url = values.get("urls").get("pbf-internal")

    file_path = join(PATH, f"{schema}.osm.pbf")
    r = requests.get(download_url, cookies=cookies, stream=True)
    total_length = int(r.headers.get("content-length"))

    with open(file_path, "wb") as f:
        dl = 0
        for data in r.iter_content(chunk_size=4096):
            dl += len(data)
            f.write(data)
            done = int(50 * dl / total_length)
            sys.stdout.write("\r[%s%s]" % ("=" * done, " " * (50 - done)))
            sys.stdout.flush()
        print("\n")

    ds = gdal.OpenEx(file_path, gdal.OF_VECTOR)

    users_count = 0
    # GetNextFeature handles interleaved reading correctly 
    users = []
    for ily in range (0, ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(ily)

        layer.ResetReading()
        feat = layer.GetNextFeature()

        while feat is not None:
            user = feat.items().get("osm_user")
            if user not in users:
                users.append(user)
                users_count += 1

            feat = layer.GetNextFeature()

    return users_count

    print("Running ogr2ogr command")
    os.system(
        f'PGPASSWORD=mj psql -h localhost -U mj -d countries -c "DROP SCHEMA {schema} CASCADE"'
    )


    os.system(
        f'PGPASSWORD=mj psql -h localhost -U mj -d countries -c "CREATE SCHEMA {schema}"'
    )

    os.system(
        f'ogr2ogr -progress -overwrite -f "PostgreSQL" PG:"host=localhost dbname=countries user=\'mj\' password=\'mj\' port=5432" {file_path} -lco SCHEMA={schema} -oo CONFIG_FILE="./customconf.ini" -lco OVERWRITE=YES'
    )

    os.system(
        f'PGPASSWORD=mj psql -h localhost -U mj -d countries -c "with t1 AS (select osm_user from bt.lines union select osm_user from bt.multilinestrings union select osm_user from bt.multipolygons union select osm_user from bt.other_relations union select osm_user from bt.points) select distinct count(osm_user) from t1;"'

    )

    os.remove(file_path)


def main():
    username = "jorgemrtnz"
    password = "Jorgemg_0327"
    consumer_url = "https://osm-internal.download.geofabrik.de/get_cookie"

    oauth_token, oauth_token_secret_encr = get_tokens(consumer_url)
    if None in [oauth_token, oauth_token_secret_encr]:
        raise ValueError(
            "oauth_token was not found in the first response by the consumer"
        )

    session = create_osm_session(username, password)
    authorize(session, oauth_token)

    cookie_text = logout(
        consumer_url, session, oauth_token, oauth_token_secret_encr
    )

    cookies = dict([cookie_text.split(";")[0].split("=", 1)])

    with open("./countries.json", "r") as f:
        countries = json.load(f)

    for name, values in list(countries.items())[:2]:
        print(f"Processing country: {name}")
        count = process_country(values, cookies)

        '''
        with open("results.csv", "a") as f:
            f.write(f"{name},{count}\n") 
        '''

if __name__ == "__main__":
    main()
