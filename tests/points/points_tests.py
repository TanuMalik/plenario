import unittest
from tests.test_fixtures.point_meta import flu_shot_meta, landmarks_meta
from plenario.models import MetaTable
from plenario.database import session, app_engine
from plenario.utils.etl import PlenarioETL
from init_db import init_master_meta_user
from sqlalchemy import Table, MetaData
from sqlalchemy.exc import NoSuchTableError


def ingest_online_from_fixture(fixture_meta):
        md = MetaTable(**fixture_meta)
        session.add(md)
        session.commit()
        point_etl = PlenarioETL(fixture_meta)
        point_etl.add()


def drop_tables(table_names):
    drop_template = 'DROP TABLE IF EXISTS {};'
    command = ''.join([drop_template.format(table_name) for table_name in table_names])
    session.execute(command)
    session.commit()


def create_dummy_census_table():
    session.execute("""
    CREATE TABLE census_blocks
    (
        geoid10 INT,
        geom geometry(MultiPolygon,4326)
    );
    """)
    session.commit()


class TimeseriesRegressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        tables_to_drop = [
            'dat_flu_shot_clinics',
            'dat_landmarks',
            'dat_master',
            'meta_master',
            'plenario_user'
        ]
        drop_tables(tables_to_drop)

        init_master_meta_user()

        # Make sure that it at least _looks_ like we have census blocks
        try:
            Table('census_blocks', MetaData(app_engine))
        except NoSuchTableError:
            create_dummy_census_table()

        ingest_online_from_fixture(flu_shot_meta)
        ingest_online_from_fixture(landmarks_meta)

    def test_basics(self):
        pass

