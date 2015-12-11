from plenario.etl.common import ETLFile
from csvkit.unicsv import UnicodeCSVReader
from sqlalchemy.exc import NoSuchTableError
from plenario.database import app_engine as engine
from plenario.utils.helpers import iter_column, slugify
import json
from sqlalchemy import Boolean, Integer, BigInteger, Float, String, Date, TIME, TIMESTAMP,\
    Table, Column, MetaData

etl_meta = MetaData()

# Can move to common?
class PlenarioETLError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


# Should StagingTable itself be a context manager? Probably.
# Missing:  Removing rows with an empty business key
#           Setting geometry coded to (0,0) to NULL

class StagingTable(object):
    def __init__(self, meta, source_path=None):
        """
        :param meta: record from MetaTable
        :param source_path: path of source file on local filesystem
        :return:
        """
        self.meta = meta

        # Get the Columns to construct our table
        try:
            # Problem: Does call to model mix SQLAlchemy sessions?
            self.cols = self._from_ingested()
        except NoSuchTableError as e:
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
                file_helper = ETLFile(source_url=meta.url)
        # TODO: Handle more specific exception
        except Exception as e:
            raise PlenarioETLError(e)

        with file_helper:
            if not self.cols:
                # We couldn't get the column metadata from an existing table or from the user.
                self.cols = self._from_inference(file_helper.handle)

            # Grab the handle to build a table from the CSV
            try:
                self.table = self._make_table(file_helper.handle, self.cols)
            except Exception as e:
                # Some stuff that could happen:
                    # There could be more columns in the source file than we expected.
                    # Some input could be malformed.
                # Can we check here to see if uniqueness constraint was violated?
                raise PlenarioETLError(e)

    def _make_table(self, f, col_info):
        # Persist an empty table eagerly
        # so that we can access it when we drop down to a raw connection.
        s_table_name = 'staging_' + self.meta.dataset_name
        # Test something out...
        self.cols.append(Column('line_num', Integer, primary_key=True))

        table = Table(s_table_name, etl_meta, *self.cols, extend_existing=True)
        table.drop(bind=engine, checkfirst=True)
        table.create(bind=engine)

        names = [c.name for c in self.cols if c.name != 'line_num']

        copy_st = "COPY {t_name} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')".\
            format(t_name=s_table_name, cols=', '.join(names))

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

    '''Three ways to make our columns.'''
    @staticmethod
    def _make_col(name, type, nullable):
        return Column(name, type, nullable=nullable)

    def _from_ingested(self):
        """
        :return: Columns that will match the table in its CSV form
        """
        col_info = self.meta.column_info()
        # Don't include the columns the ingested tables have for bookkeeping
        stripped = [c for c in col_info if c.name not in ['geom', 'point_date']]
        # Build up the columns. For 'point_id', use the original name.
        id_col_name = self.meta.business_key
        cols = [self._make_col(id_col_name, c.type, c.nullable) if c.name == 'point_id'
                else self._make_col(c.name, c.type, c.nullable)
                for c in stripped]
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


class UpsertFinder(object):

    def __init__(self, staging, existing):
        self.staging = staging
        self.existing = existing

    def find_new(self):
        # Generate table whose rows have an ID not present in the existing table
        # If there are duplicates in the staging table, grab the one lowest in the file
        # Will I need to do the line_num thing?
        pass

    def insert_new(self):
        pass

