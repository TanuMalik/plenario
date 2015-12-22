from plenario.etl.common import ETLFile
from csvkit.unicsv import UnicodeCSVReader
from sqlalchemy.exc import NoSuchTableError
from plenario.database import app_engine as engine, session
from plenario.utils.helpers import iter_column, slugify
import json
from sqlalchemy import Boolean, Integer, BigInteger, Float, String, Date, TIME, TIMESTAMP,\
    Table, Column, MetaData
from sqlalchemy import select, func, text, delete
from geoalchemy2 import Geometry
from plenario.models import MetaTable
from datetime import datetime


# Can move to common?
class PlenarioETLError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class PlenarioETL(object):
    def __init__(self, metadata, source_path=None):
        self.metadata = metadata
        self.staging_table = StagingTable(self.metadata, source_path=source_path)

    def add(self):
        new_table = self.staging_table.create_new()
        update_meta(new_table)

    def update(self):
        existing_table = self.metadata.point_table
        self.staging_table.insert_into(existing_table)
        update_meta(existing_table)


# Should StagingTable itself be a context manager? Probably.
class StagingTable(object):
    def __init__(self, meta, source_path=None):
        """
        :param meta: record from MetaTable
        :param source_path: path of source file on local filesystem
        :return:
        """
        # TODO: Must change these var names
        self.meta = meta
        self.md = MetaData()

        # Get the Columns to construct our table
        try:
            # Problem: Does call to model mix SQLAlchemy sessions?
            self.cols = self._from_ingested()
        except NoSuchTableError:
            # This must be the first time we're ingesting the table
            if meta.contributed_data_types:
                types = json.loads(meta.contributed_data_types)
                self.cols = self._from_contributed(types)
            else:
                self.cols = None

        # Retrieve the source file
        try:
            if source_path:  # Local ingest
                file_helper = ETLFile(source_path=source_path)
            else:  # Remote ingest
                file_helper = ETLFile(source_url=meta.source_url)
        # TODO: Handle more specific exception
        except Exception as e:
            raise PlenarioETLError(e)

        with file_helper:
            if not self.cols:
                # We couldn't get the column metadata from an existing table or from the user.
                self.cols = self._from_inference(file_helper.handle)

            # Grab the handle to build a table from the CSV
            try:
                self.table = self._make_table(file_helper.handle)
                self._kill_dups(self.table)
                # Remove malformed rows (by business logic) here.
            except Exception as e:
                # Some stuff that could happen:
                    # There could be more columns in the source file than we expected.
                    # Some input could be malformed.
                # Can we check here to see if uniqueness constraint was violated?
                raise PlenarioETLError(e)

    def _make_table(self, f):
        # Persist an empty table eagerly
        # so that we can access it when we drop down to a raw connection.
        s_table_name = 'staging_' + self.meta.dataset_name
        # Test something out...
        self.cols.append(Column('line_num', Integer, primary_key=True))

        table = Table(s_table_name, self.md, *self.cols, extend_existing=True)
        table.drop(bind=engine, checkfirst=True)
        table.create(bind=engine)

        # Fill in the columns we expect from the CSV.
        # line_num will get a sequence by default.
        names = [c.name for c in self.cols if c.name != 'line_num']
        copy_st = "COPY {t_name} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')".\
            format(t_name=s_table_name, cols=', '.join(names))
        # print copy_st

        # In order to issue a COPY, we need to drop down to the psycopg2 DBAPI.
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cursor:
                cursor.copy_expert(copy_st, f)
                conn.commit()
                return table
        except Exception as e:  # When the bulk copy fails on _any_ row, roll back the entire operation.
            raise PlenarioETLError(e)
        finally:
            conn.close()

    def _kill_dups(self, t):

        del_stmt = '''
        DELETE FROM {table}
        WHERE line_num NOT IN (
          SELECT MIN(line_num) FROM {table}
          GROUP BY {bk}
        )
        '''.format(table=t.name, bk=self.meta.business_key)

        session.execute(del_stmt)
        try:
            session.commit()
        except:
            session.rollback()


    '''Three ways to make our columns.'''
    @staticmethod
    def _make_col(name, type, nullable):
        return Column(name, type, nullable=nullable)

    def _copy_col(self, col):
        return self._make_col(col.name, col.type, col.nullable)

    def _from_ingested(self):
        """
        :return: Columns that will match the table in its CSV form
        """
        ingested_cols = self.meta.column_info()
        # Don't include the geom and point_date columns.
        # They're derived from the source data and won't be present in the source CSV
        original_cols = [c for c in ingested_cols if c.name not in ['geom', 'point_date']]
        # Finally, the point_id column is present in the source, but under its original name.
        cols = [self._make_col(self.meta.business_key, c.type, c.nullable) if c.name == 'point_id'
                else self._make_col(c.name, c.type, c.nullable)
                for c in original_cols]

        return cols

    def _from_inference(self, f):
        """
        :param f: open file handle to CSV
        """
        reader = UnicodeCSVReader(f)
        header = map(slugify, reader.next())

        cols = []
        for col_idx, col_name in enumerate(header):
            col_type, nullable = iter_column(col_idx, f)
            cols.append(self._make_col(col_name, col_type, nullable))
        return cols

    def _from_contributed(self, data_types):
        """
        :param data_types: List of dictionaries, each of which has 'field_name' and 'data_type' fields.
        """
        COL_TYPES = {
            'boolean': Boolean,
            'integer': Integer,
            'big_integer': BigInteger,
            'float': Float,
            'string': String,
            'date': Date,
            'time': TIME,
            'timestamp': TIMESTAMP,
            'datetime': TIMESTAMP,
        }

        cols = [self._make_col(c['field_name'], COL_TYPES[c['data_type']], True) for c in data_types]
        return cols

    def _date_selectable(self):
        """
        Make a selectable where we take the dataset's temporal column
        And cast every record to a Postgres TIMESTAMP
        """

        return func.cast(self.table.c[self.meta.observed_date], TIMESTAMP).\
                label('point_date')

    def _geom_selectable(self):
        """
        Derive selectable with a PostGIS point in 4326 (naive lon-lat) projection
        derived from either the latitude and longitude columns or single location column
        """
        t = self.table
        m = self.meta

        if m.latitude and m.longitude:
            geom_col = func.ST_SetSRID(func.ST_Point(t.c[m.longitude], t.c[m.latitude]),
                                       4326)

        elif m.location:
            geom_col = text(
                    '''SELECT ST_PointFromText('POINT(' || subq.lon || ' ' || subq.lat || ')', 4326) \
                          FROM (SELECT a[1] AS lon, a[2] AS lat
                                  FROM (SELECT regexp_matches({}, '\((.*), (.*)\)') FROM {} AS FLOAT8(a))
                                AS subq)
                       AS geom;'''.format(t.c[m.location], 'staging_' + m.dataset_name))
        else:
            raise PlenarioETLError('Staging table does not have geometry information.')

        return geom_col

    def _derived_cte(self, existing):
        """
        Construct a gnarly Common Table Expression that generates all of the columns
        that we want to derive from
        """
        t = self.table
        m = self.meta

        geom_sel = self._geom_selectable()
        date_sel = self._date_selectable()

        # The select_from and where clauses ensure we're only looking at records
        # that don't have a unique ID that's present in the existing dataset.
        #
        # From the records with an ID not in the existing dataset,
        # generate new columns with a geom and timestamp.
        #
        # Finally, include the id itself in the common table expression to join to the staging table.
        cte = select([t.c[m.business_key].label('id'),
                      geom_sel.label('geom'),
                      date_sel.label('point_date')]).\
            select_from(t.outerjoin(existing, t.c[m.business_key] == existing.c.point_id)).\
            where(existing.c.point_id == None).\
            distinct().\
            alias('id_cte')

        return cte

    def create_new(self):
        # The columns we're taking straight from the source file
        verbatim_cols = []
        for c in self.cols:
            if c.name != 'line_num':
                if c.name == self.meta.business_key:
                    # On second thought, imposing the point_id name was a mistake.
                    verbatim_cols.append(Column('point_id', c.type, primary_key=True))
                else:
                    verbatim_cols.append(self._copy_col(c))

        derived_cols = [
            Column('point_date', TIMESTAMP, nullable=False),
            Column('geom', Geometry('POINT', srid=4326), nullable=True)
        ]

        new_table = Table(self.meta.dataset_name, self.md, *(verbatim_cols + derived_cols))
        new_table.create(engine)
        self.insert_into(new_table)
        return new_table

    def insert_into(self, existing):
        # Generate table whose rows have an ID not present in the existing table
        # If there are duplicates in the staging table, grab the one lowest in the file
        cte = self._derived_cte(existing)
        ins_cols = []
        for c in self.cols:
            if c.name == 'line_num':
                continue
            elif c.name == self.meta.business_key:
                ins_cols.append(c.label('point_id'))
            else:
                ins_cols.append(c)

        ins_cols += [cte.c.geom, cte.c.point_date]

        # The cte only includes rows with business keys that weren't present in the existing table
        # Here, we also restrict our selection to rows that are of the lowest dup_ver (highest in the source file)
        sel = select(ins_cols).\
            select_from(cte.join(self.table, cte.c.id == self.table.c[self.meta.business_key])).\
            where(cte.c.point_date != None)
        # Insert all the original and derived columns into the table
        ins = existing.insert().from_select(ins_cols, sel)
        engine.execute(ins)
        self._null_malformed_geoms(existing)

    @staticmethod
    def _null_malformed_geoms(existing):
        # We decide to set the geom to NULL when the given lon/lat is (0,0) (e.g. off the coast of Africa).
        upd = existing.update().values(geom=None).\
            where(existing.c.geom == select([func.ST_SetSRID(func.ST_MakePoint(0, 0), 4326)]))
        engine.execute(upd)


def update_meta(table):
    record = MetaTable.get_by_dataset_name(table.name)
    record.update_date_added()
    record.obs_from, record.obs_to = session.query(func.min(table.c.point_date),
                                                   func.max(table.c.point_date)).first()
    # Get an envelope over all points in the dataset as binary WKB
    bbox = session.query(func.ST_SetSRID(
                                         func.ST_Envelope(func.ST_Union(table.c.geom)),
                                         4326
                                         )).first()[0]
    record.bbox = bbox
    session.add(record)
    try:
        session.commit()
    # TODO: Catch more specific
    except Exception:
        session.rollback()
