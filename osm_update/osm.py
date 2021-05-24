import osmium
from sqlalchemy import create_engine, Column, Float, BigInteger, String

from sqlalchemy.dialects.postgresql import HSTORE, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError

from argparse import ArgumentParser

DB_SCHEMA = "example"

engine = create_engine("postgresql://mj:mj@localhost:5432/osm_countries")
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

import psycopg2

conn = psycopg2.connect("postgresql://mj:mj@localhost:5432/osm_countries")
cur = conn.cursor()


class Node(Base):
    __tablename__ = "osm_nodes"
    __table_args__ = {"schema": DB_SCHEMA}

    osm_id = Column(BigInteger, primary_key=True)
    username = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    tags = Column(HSTORE)

class Way(Base):
    __tablename__ = "osm_ways"
    __table_args__ = {"schema": DB_SCHEMA}

    osm_id = Column(BigInteger, primary_key=True)
    username = Column(String, nullable=False)
    node_refs = Column(ARRAY(BigInteger), default=[])
    tags = Column(HSTORE)

class Relation(Base):
    __tablename__ = "osm_relations"
    __table_args__ = {"schema": DB_SCHEMA}

    osm_id = Column(BigInteger, primary_key=True)
    username = Column(String, nullable=False)
    members = Column(ARRAY(HSTORE))
    tags = Column(HSTORE)


def populate_database(osm_file):
    h = PopulateHandler()

    h.apply_file(osm_file)


class PopulateHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)

    @staticmethod
    def to_hstore_str(tags):
        values = list()
        for k, v in dict(tags).items():
            value = v.replace('"', '\\"').replace("'", "''")
            values.append(f'"{k}" => "{value}"')

        tags = ",".join(values)
        tags = f"'{tags}'::hstore"

        return tags

    def node(self, elm):
        items = dict(id=elm.id, user=elm.user, lat=elm.location.lat, lng=elm.location.lon, tags=PopulateHandler.to_hstore_str(elm.tags))
        query = "INSERT INTO example.osm_nodes(osm_id, username, lat, lon, tags) VALUES ({id},'{user}',{lat},{lng},{tags})".format(**items)

        cur.execute(query)
        conn.commit()

    def way(self, elm):
        node_refs = ",".join([str(n.ref) for n in elm.nodes])
        node_refs = f"ARRAY[{node_refs}]"

        items = dict(id=elm.id, user=elm.user, node_refs=node_refs, tags=PopulateHandler.to_hstore_str(elm.tags))
        query = "INSERT INTO example.osm_ways(osm_id, username, node_refs, tags) VALUES ({id},'{user}',{node_refs},{tags})".format(**items)

        cur.execute(query)
        conn.commit()

    def relation(self, elm):
        members_dict = [{'ref': e.ref, 'type': e.type, 'role': e.role} for e in elm.members]
        members_dict = [",".join([f'"{k}" => "{v}"' for k,v in m.items()]) for m in members_dict]

        members_dict = ",".join([f"'{m}'::hstore" for m in members_dict])

        members = f"ARRAY[{members_dict}]"
        tags = PopulateHandler.to_hstore_str(elm.tags)

        items = dict(id=elm.id, user=elm.user, tags=tags, members=members)
        query = "INSERT INTO example.osm_relations(osm_id, username, members, tags) VALUES ({id},'{user}',{members},{tags})".format(**items)

        cur.execute(query)
        conn.commit()


def main():
    # Database check.
    try:
        engine.execute(CreateSchema(DB_SCHEMA))
    except ProgrammingError:
        pass

    # Create tables.
    Base.metadata.create_all(engine)

    parser = ArgumentParser(description="Process osm files into database")
    parser.add_argument("-p", "--populate", default=False, action="store_true")
    parser.add_argument("osm_file", type=str)

    args = parser.parse_args()
    if args.populate is True:
        populate_database(args.osm_file)


if __name__ == "__main__":
    main()
