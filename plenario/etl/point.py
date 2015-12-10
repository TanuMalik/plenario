from plenario.etl.common import ETLFile
from collections import namedtuple
from csvkit.unicsv import UnicodeCSVReader
from sqlalchemy.exc import NoSuchTableError
from plenario.utils.helpers import iter_column, slugify
import json
from sqlalchemy import Boolean, Integer, BigInteger, Float, String, Date, TIME, TIMESTAMP


# DRY: also in models
ColumnInfo = namedtuple('ColumnInfo', ['name', 'type', 'nullable'])

# Should StagingTable itself be a context manager? Probably.
class StagingTable(object):
    def __init__(self, meta, source_path=None):
        """
        :param meta: record from MetaTable
        :param source_path: path of source file on local filesystem
        :return:
        """
        # Init file_helper first to fail fast
        # Need try-except to bail when we can't find the file.
        if source_path:
            # Local ingest
            file_helper = ETLFile(source_path=source_path)
        else:
            # Remote ingest
            file_helper = ETLFile(source_url=meta.url)

        # Can we gather the profile of the table's columns without scanning the file?
        try:
            # Problem: Does this mix SQLAlchemy sessions?
            self.col_info = meta.column_info()
            print self.col_info[0].type
        except NoSuchTableError as e:
            print e
            # This must be the first time we're ingesting the table
            if meta.contributed_data_types:
                types = json.loads(meta.contributed_data_types)
                self.col_info = self._parse_contributed(types)
            else:
                self.col_info = None

        with file_helper:
            f = file_helper.handle

            if not self.col_info:
                # We couldn't get the column metadata from an existing table or from the user.
                self.col_info = self._infer_columns(f)
                f.seek(0)

            # Grab the handle to build a table from the CSV

        ''' try:
                table = self._make_table(f, col_info)
                f.seek(0)
            except Exception:
                # Some stuff that could happen:
                    # There could be more columns in the source file than we expected.
                    # Some input could be malformed.
                # Can we check here to see if uniqueness constraint was violated?
                pass'''

        self.table = None

    def _infer_columns(self, f):
        """
        :param f: open file handle to CSV
        """
        reader = UnicodeCSVReader(f)
        header = map(slugify, reader.next())

        col_info = []
        for col_idx, col_name in enumerate(header):
            col_type, nullable = iter_column(col_idx, f)
            col_info.append(ColumnInfo(col_name, col_type, nullable))
        return col_info

    def _make_table(self, f, col_info):
        table = None
        return table

    def _parse_contributed(self, data_types):
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

        col_info = [ColumnInfo(c['field_name'], COL_TYPES[c['data_type']], True) for c in data_types]
        return col_info
