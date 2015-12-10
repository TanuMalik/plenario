from unittest import TestCase
from plenario.models import MetaTable
from plenario.database import session, Base, app_engine
import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, Date, Float, String
from geoalchemy2 import Geometry
from plenario.etl.point import StagingTable, ColumnInfo
import os

pwd = os.path.dirname(os.path.realpath(__file__))
fixtures_path = os.path.join(pwd, '../test_fixtures')


class StagingTableTests(TestCase):
    """
    Given a dataset is present in MetaTable,
    can we grab a current csv of the underlying data from wherever it lives
    and then make that into a free-standing table?
    """

    def setUp(self):
        # Make two new MetaTable objects
        self.unloaded_meta = MetaTable(url='nightvale.gov/events.csv',
                                       human_name='Community Radio Events',
                                       business_key='Event Name',
                                       observed_date='Date',
                                       latitude='lat', longitude='lon',
                                       approved_status=True)

        # For one of those entries, create a point table in the database (we'll eschew the dat_ convention)
        self.existing_meta = MetaTable(url='nightvale.gov/dogpark.csv',
                                       human_name='Dog Park Permits',
                                       business_key='Hooded Figure ID',
                                       observed_date='Date',
                                       latitude='lat', longitude='lon',
                                       approved_status=False)

        self.existing_table = sa.Table('dog_park_permits', Base.metadata,
                                       Column('point_id', Integer, primary_key=True),
                                       Column('date', Date, nullable=False),
                                       Column('lat', Float, nullable=False),
                                       Column('lon', Float, nullable=False),
                                       Column('geom', Geometry('POINT', srid=4326), nullable=False),
                                       extend_existing=True)

        Base.metadata.create_all(bind=app_engine)

    def test_col_info_infer(self):
        source_path = os.path.join(fixtures_path, 'community_radio_events.csv')
        s_table = StagingTable(self.unloaded_meta, source_path=source_path)
        cols = s_table.col_info
        self.assertEqual(len(cols), 4)
        expected_cols = [ColumnInfo('lat', Float, False), ColumnInfo('lon', Float, False),
                         ColumnInfo('date', Date, False), ColumnInfo('event_name', String, False)]
        for expected in expected_cols:
            self.assertIn(expected, cols)

    def test_col_info_existing(self):
        source_path = os.path.join(fixtures_path, 'dog_park_permits.csv')
        s_table = StagingTable(self.existing_meta, source_path=source_path)
        observed_col_names = [col.name for col in s_table.col_info]
        expected_col_names = ['lat', 'lon', 'date', 'point_id', 'geom']
        self.assertEqual(set(observed_col_names), set(expected_col_names))
        # I also wanted to test that the Postgres-dialect-specific types that got generated
        # matched the general SQLAlchemy types specified in the table defnition.
        # But that got hairy.

    def test_col_info_provided(self):
        pass

    def test_new_table(self):
        # For the entry in MetaTable without a table, create a staging table.
        # We'll need to read from a fixture csv.
        source_path = os.path.join(fixtures_path, 'community_radio_events.csv')
        s_table = StagingTable(self.unloaded_meta, source_path=source_path)
        sel = sa.select([sa.func.count(s_table.table)])
        # Fun fact. If s_table.table is None, this statement is "SELECT count(*);" which evaluates to 1 (O_o)
        count = session.execute(sel).first()[0]
        self.assertEqual(count, 5)

    def test_extra_column_failure(self):
        # With a fixture CSV that has one more column than the one that we inserted in the databse,
        # try to create the staging table and expect an Exception
        self.assert_(False)

    def test_existing_table(self):
        # With a fixture CSV whose columns match the existing dataset,
        # create a staging table.
        self.assert_(False)

    # A nice optimization will be to do a simple hash check,
    # but that can wait for a future release
    '''def test_existing_table_changed(self):
        pass

    def test_existing_table_not_changed(self):
        pass'''


class UpsertTableTests(TestCase):
    """
    Given a staging table that matches the columns of an existing table,
    can we upsert new and modified records into the existing table?
    And what happens when we need to bail?
    """

    # Maybe define a really simple 10-line table inline

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
