from unittest import TestCase
from plenario.models import MetaTable
from plenario.database import session, Base, app_engine
import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, Date, Float, String, TIMESTAMP
from geoalchemy2 import Geometry
from plenario.etl.point import StagingTable
import os
import json
from datetime import date

pwd = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(pwd, '../test_fixtures')


class StagingTableTests(TestCase):
    """
    Given a dataset is present in MetaTable,
    can we grab a current csv of the underlying data from wherever it lives
    and then make that into a free-standing table?
    """
    @classmethod
    def setUpClass(cls):
        cls.dog_path = os.path.join(fixtures_path, 'dog_park_permits.csv')
        cls.radio_path = os.path.join(fixtures_path, 'community_radio_events.csv')

        # Make two new MetaTable objects
        cls.unloaded_meta = MetaTable(url='nightvale.gov/events.csv',
                                      human_name='Community Radio Events',
                                      business_key='Event Name',
                                      observed_date='Date',
                                      latitude='lat', longitude='lon',
                                      approved_status=True)
        cls.expected_radio_col_names = ['lat', 'lon', 'event_name', 'date', 'line_num']

        cls.existing_meta = MetaTable(url='nightvale.gov/dogpark.csv',
                                      human_name='Dog Park Permits',
                                      business_key='Hooded Figure ID',
                                      observed_date='Date',
                                      latitude='lat', longitude='lon',
                                      approved_status=False)
        cls.expected_dog_col_names = ['lat', 'lon', 'hooded_figure_id', 'date', 'line_num']

        # For one of those entries, create a point table in the database (we'll eschew the dat_ convention)
        cls.existing_table = sa.Table('dog_park_permits', Base.metadata,
                                      Column('point_id', Integer, primary_key=True),
                                      Column('point_date', TIMESTAMP, nullable=False),
                                      Column('date', Date, nullable=True),
                                      Column('lat', Float, nullable=False),
                                      Column('lon', Float, nullable=False),
                                      Column('geom', Geometry('POINT', srid=4326), nullable=True),
                                      extend_existing=True)

        cls.existing_table.drop(checkfirst=True)
        Base.metadata.create_all(bind=app_engine)

        ins = cls.existing_table.insert().values(point_id=1,
                                                  point_date=date(2015, 1, 2),
                                                  lon=-87.6495076896,
                                                  lat=41.7915865543,
                                                  geom=None)
        app_engine.execute(ins)

    @classmethod
    def tearDownClass(cls):
        cls.existing_table.drop(checkfirst=True)

    def tearDown(self):
        session.close()

    '''
    Do the names of created columns match what we expect?
    Would be nice to check types too, but that was too fragile.
    '''
    @staticmethod
    def extract_names(columns):
        return [c.name for c in columns]

    def test_col_info_infer(self):
        s_table = StagingTable(self.unloaded_meta, source_path=self.radio_path)
        observed_names = self.extract_names(s_table.cols)
        self.assertEqual(set(observed_names), set(self.expected_radio_col_names))

    def test_col_info_existing(self):
        s_table = StagingTable(self.existing_meta, source_path=self.dog_path)
        observed_col_names = self.extract_names(s_table.cols)
        self.assertEqual(set(observed_col_names), set(self.expected_dog_col_names))

    def test_col_info_provided(self):
        # The frontend should send back strings compatible with the COL_VALUES in etl.point
        col_info_raw = [('event_name', 'string'),
                        ('date', 'date'),
                        ('lat', 'float'),
                        ('lon', 'float')]
        stored_col_info = [{'field_name': name, 'data_type': d_type}
                           for name, d_type in col_info_raw]
        self.unloaded_meta.contributed_data_types = json.dumps(stored_col_info)
        s_table = StagingTable(self.unloaded_meta, source_path=self.radio_path)

        observed_names = self.extract_names(s_table.cols)
        self.assertEqual(set(observed_names), set(self.expected_radio_col_names))

    '''
    Are the files ingested as we expect?
    '''

    def test_staging_new_table(self):
        # For the entry in MetaTable without a table, create a staging table.
        # We'll need to read from a fixture csv.
        s_table = StagingTable(self.unloaded_meta, source_path=self.radio_path)
        all_rows = session.execute(s_table.table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)

    def test_staging_existing_table(self):
        # With a fixture CSV whose columns match the existing dataset,
        # create a staging table.
        s_table = StagingTable(self.existing_meta, source_path=self.dog_path)
        all_rows = session.execute(s_table.table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)

    def test_insert_data(self):
        staging = StagingTable(self.existing_meta, source_path=self.dog_path)
        existing = self.existing_table
        staging.insert_into(existing)
        all_rows = session.execute(existing.select()).fetchall()
        self.assertEqual(len(all_rows), 5)

    def test_new_table(self):
        staging = StagingTable(self.unloaded_meta, source_path=self.radio_path)
        new_table = staging.create_new()
        all_rows = session.execute(new_table.select()).fetchall()
        self.assertEqual(len(all_rows), 5)


    '''
    Does the table disappear once it goes out of context?
    '''

    ''' Eh. Later
    def test_self_cleaning(self):
        pass'''


    '''def test_extra_column_failure(self):
        # With a fixture CSV that has one more column than the one that we inserted in the databse,
        # try to create the staging table and expect an Exception
        self.assert_(False)'''


class UpsertTableTests(TestCase):
    """
    Given a staging table that matches the columns of an existing table,
    can we upsert new and modified records into the existing table?
    And what happens when we need to bail?

    I need a mechanism to create a "new" table and an "update" table from two other tables.

    new_t = find_new_rows(staging, existing)
    update_t = find_update_rows(staging, existing)

    So I'll need to compare the unique id of the staging table to the unique id of the existing table.

    """

    # Maybe define a really simple 10-line table inline

    def setUp(self):
        pass

    def test_no_overlap(self):
        pass

    def test_update_records(self):
        pass

    def test_insert_new_records(self):
        pass

    def test_fail_on_duplicates(self):
        pass


class UpdateMetaTests(TestCase):
    pass


class UpdateJoinsTests(TestCase):
    pass
