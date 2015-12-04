from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, make_cache_key, \
    dthandler, make_csv, extract_first_geometry_fragment, make_fragment_str, RESPONSE_LIMIT
from flask import request, make_response
import dateutil.parser
from datetime import timedelta, datetime
from plenario.models import MetaTable
from itertools import groupby
from operator import itemgetter
import json
from sqlalchemy import Table
import sqlalchemy as sa
from sqlalchemy.exc import NoSuchTableError
from plenario.database import session, Base, app_engine as engine
import shapely.wkb

VALID_AGG = ['day', 'week', 'month', 'quarter', 'year']


class ParamValidator(object):

    def __init__(self, dataset_name=None):
        # Maps param keys to functions that validate and transform its string value.
        # Each transform returns (transformed_value, error_string)
        self.transforms = {}
        # Map param keys to usable values.
        self.vals = {}
        # Let the caller know which params we ignored.
        self.warnings = []

        if dataset_name:
            # Throws NoSuchTableError. Should be caught by caller.
            self.dataset = Table('dat_' + dataset_name, Base.metadata,
                            autoload_with=engine, extend_existing=True)
            self.cols = self.dataset.columns.keys()
            # SQLAlchemy boolean expressions
            self.conditions = []

    def set_optional(self, name, transform, default):
        self.vals[name] = default
        self.transforms[name] = transform

        # For call chaining
        return self

    def validate(self, params):
        for k, v in params.items():
            if k in self.transforms.keys():
                # k is a param name with a defined transformation
                val, err = self.transforms[k](v)
                if err:
                    # v wasn't a valid string for this param name
                    return err
                # Override the default with the transformed value.
                self.vals[k] = val
                continue

            elif self.cols:
                # Maybe k specifies a condition on the dataset
                cond, err = self._make_condition(k, v)
                if cond:
                    self.conditions.append(cond)
                    continue
                elif err:
                    # Valid field was specified, but operator was malformed
                    return err
                # else k wasn't an attempt at setting a condition

            # This param is neither present in the optional params
            # nor does it specify a field in this dataset.
            warning = 'Unused parameter value "{}={}"'.format(k, v)
            self.warnings.append(warning)

        self._eval_defaults()

    def _eval_defaults(self):
        """
        Replace every value in vals that is callable with the returned value of that callable.
        Lets us lazily evaluate dafaults only when they aren't overridden.
        """
        for k, v in self.vals.items():
            if hasattr(v, '__call__'):
                self.vals[k] = v()

    # Map codes we accept in API docs to sqlalchemy function names
    field_ops = {
        'gt': '__gt__',
        'ge': '__ge__',
        'lt': '__lt__',
        'le': '__le__',
        'ne': '__ne__',
        'like': 'like',
        'ilike': 'ilike',
    }

    def _make_condition(self, k, v):
        # Generally, we expect the form k = [field]__[op]
        # Can also be just [field] in the case of simple equality
        tokens = k.split('__')
        field = tokens[0]
        if field not in self.cols:
            # Don't make a condition.
            # But nothing went wrong, so error is None too.
            return None, None

        col = self.dataset.columns.get(field)

        if len(tokens) == 1:
            # One token? Then it's an equality operation of the form k=v
            cond = col == v
            return cond, None
        elif len(tokens) == 2:
            # Two tokens? Then it's of the form [field]__[op_code]=v
            op_code = tokens[1]
            if op_code == 'in':
                # TODO: change documentation to reflect expected input format
                cond = col.in_(v.split(','))
            else:
                try:
                    op_func = ParamValidator.field_ops[op_code]
                    cond = getattr(col, op_func)(v)
                except AttributeError:
                    error_msg = "Invalid dataset field operator: {} called in {}={}".format(op_code, k, v)
                    return None, error_msg
            return cond, None
        else:
            error_msg = "Too many arguments on dataset field {}={}\n Expected [field]__[operator]=value".format(k, v)
            return None, error_msg


def agg_validator(agg_str):
    if agg_str in VALID_AGG:
        return agg_str, None
    else:
        error_msg = '{} is not a valid unit of aggregation. Plenario accepts {}'\
                    .format(agg_str, ','.join(VALID_AGG))
        return None, error_msg


def date_validator(date_str):
    try:
        date = dateutil.parser.parse(date_str)
        return date, None
    except (ValueError, OverflowError):
        error_msg = 'Could not parse date string {}'.format(date_str)
        return None, error_msg


def list_of_datasets_validator(list_str):
    table_names = list_str.split(',')
    if not len(table_names) > 1:
        error_msg = "Expected comma-separated list of computer-formatted dataset names. Couldn't parse {}".format(list_str)
        return None, error_msg
    return table_names, None


def make_format_validator(valid_formats):
    """
    :param valid_formats: A list of strings that are acceptable types of data formats.
    :return: a validator function usable by ParamValidator
    """

    def format_validator(format_str):
        if format_str in valid_formats:
            return format_str, None
        else:
            error_msg = '{} is not a valid output format. Plenario accepts {}'\
                        .format(format_str, ','.join(valid_formats))
            return error_msg, None

    return format_validator


def geom_validator(geojson_str):
    # Only extracts first geometry fragment as dict.
    try:
        fragment = extract_first_geometry_fragment(geojson_str)
        return fragment, None
    except ValueError:
        error_message = "Could not parse as geojson: {}".format(geojson_str)
        return None, error_message


def int_validator(int_str):
    try:
        num = int(int_str)
        assert(num > 0)
        return num, None
    except (ValueError, AssertionError):
        error_message = "Could not parse as positive integer: {}".format(int_str)
        return None, error_message


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def timeseries():
    resp = {
        'meta': {
            'status': '',
            'message': '',
        },
        'objects': [],
    }

    validator = ParamValidator()\
        .set_optional('agg', agg_validator, 'day')\
        .set_optional('data_type', make_format_validator(['json', 'csv']), 'json')\
        .set_optional('dataset_name__in', list_of_datasets_validator, lambda: MetaTable.index())\
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90))\
        .set_optional('obs_date__le', date_validator, datetime.now())\
        .set_optional('location_geom__within', geom_validator, None)\
        .set_optional('buffer', int_validator, 100)

    err = validator.validate(request.args)
    if err:
        resp['meta']['status'] = 'error'
        resp['meta']['message'] = err
        resp['meta']['query'] = request.args
        resp = make_response(json.dumps(resp, default=dthandler), 400)
        resp.headers['Content-Type'] = 'application/json'
        return resp

    # Geometry is an optional parameter.
    # If it was provided, convert the polygon or linestring to a postgres-ready form.
    geom = validator.vals.get('location_geom__within', None)
    if geom:
        buffer = validator.vals['buffer']
        # Should probably catch a shape exception here
        geom = make_fragment_str(geom, buffer)

    table_names = validator.vals['dataset_name__in']
    start_date = validator.vals['obs_date__ge']
    end_date = validator.vals['obs_date__le']
    agg = validator.vals['agg']

    # Only examine tables that have a chance of containing records within the date and space boundaries.
    table_names = MetaTable.narrow_candidates(table_names, start_date, end_date, geom)

    panel = MetaTable.timeseries_all(table_names=table_names,
                                     agg_unit=agg,
                                     start=start_date,
                                     end=end_date,
                                     geom=geom)

    resp['objects'] = panel
    # We're gonna mutate the query, so we have to make a copy
    resp['meta']['query'] = request.args.copy()
    if geom:
        resp['meta']['query']['location_geom__within'] = geom
    resp['meta']['query']['agg'] = agg
    resp['meta']['status'] = 'ok'

    datatype = validator.vals['data_type']
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=dthandler), 200)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':

        # response format
        # temporal_group,dataset_name_1,dataset_name_2
        # 2014-02-24 00:00:00,235,653
        # 2014-03-03 00:00:00,156,624

        fields = ['temporal_group']
        for o in resp['objects']:
            fields.append(o['dataset_name'])

        csv_resp = []
        i = 0
        for k,g in groupby(resp['objects'], key=itemgetter('dataset_name')):
            l_g = list(g)[0]

            j = 0
            for row in l_g['items']:
                # first iteration, populate the first column with temporal_groups
                if i == 0:
                    csv_resp.append([row['datetime']])
                csv_resp[j].append(row['count'])
                j += 1
            i += 1

        csv_resp.insert(0, fields)
        csv_resp = make_csv(csv_resp)
        resp = make_response(csv_resp, 200)
        resp.headers['Content-Type'] = 'text/csv'
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.csv' % filedate
    return resp


@cache.cached(timeout=CACHE_TIMEOUT, key_prefix=make_cache_key)
@crossdomain(origin="*")
def detail():

    resp = {
        'meta': {
            'status': '',
            'message': '',
        },
        'objects': [],
    }

    def make_error(msg):
        resp['meta']['status'] = 'error'
        resp['meta']['message'] = msg
        resp['meta']['query'] = request.args
        return make_response(json.dumps(resp), 400)

    raw_query_params = request.args.copy()

    # First, make sure name of dataset was provided...
    try:
        dataset_name = raw_query_params['dataset_name']
    except KeyError:
        return make_error("'dataset_name' is required")

    # and that we have that dataset.
    try:
        validator = ParamValidator(dataset_name)
    except NoSuchTableError:
        return make_error("Cannot find dataset named {}".format(dataset_name))

    validator\
        .set_optional('obs_date__ge', date_validator, datetime.now() - timedelta(days=90))\
        .set_optional('obs_date__le', date_validator, datetime.now())\
        .set_optional('location_geom__within', geom_validator, None)\
        .set_optional('offset', int_validator, 0)\
        .set_optional('data_type', make_format_validator(['json', 'csv']), 'json')

    # If any optional parameters are malformed, we're better off bailing and telling the user
    # than using a default and confusing them.
    err = validator.validate(raw_query_params)
    if err:
        return make_error(err)

    # I feel dirty constructing a big SQL call in the API view
    # But let's leave a more ambitious cleanup for another day.
    dset = validator.dataset
    q = session.query(dset)
    if validator.conditions:
        q = q.filter(*validator.conditions)

    start_date = validator.vals['obs_date__ge']
    end_date = validator.vals['obs_date__le']
    q = q.filter(dset.c.point_date >= start_date)\
         .filter(dset.c.point_date <= end_date)

    # If user provided a geom,
    geom = validator.vals.get('location_geom__within', None)
    if geom:
        # make it a str ready for postgres.
        geom = make_fragment_str(geom)
        # Assumption: geom column holds PostGIS points
        q = q.filter(dset.c.geom.ST_Within(sa.func.ST_GeomFromGeoJSON(geom)))


    # Page in RESPONSE_LIMIT chunks
    offset = validator.vals['offset']
    q = q.limit(RESPONSE_LIMIT)
    if offset > 0:
        q = q.offset(offset)

    col_names = validator.cols

    geom_idx = col_names.index('geom')
    rows = []

    for record in q.all():
        row = list(record)
        row[geom_idx] = shapely.wkb.loads(row[geom_idx].desc, hex=True).__geo_interface__
        rows.append(row)

    datatype = validator.vals['data_type']


    # TODO update docs to reflect that geojson is unsupported.
    if datatype == 'json':
        for row in rows:
            fields = {col: val for col, val in zip(col_names, row)}
            resp['objects'].append(fields)

        resp['meta']['total'] = len(resp['objects'])
        resp['meta']['query'] = validator.vals
        resp = make_response(json.dumps(resp, default=dthandler), 200)
        resp.headers['Content-Type'] = 'application/json'

    elif datatype == 'csv':
        csv_resp = [col_names] + rows
        resp = make_response(make_csv(csv_resp), 200)
        dname = raw_query_params['dataset_name']
        filedate = datetime.now().strftime('%Y-%m-%d')
        resp.headers['Content-Type'] = 'text/csv'
        resp.headers['Content-Disposition'] = 'attachment; filename=%s_%s.csv' % (dname, filedate)

    return resp


def parse_join_query(params):
    queries = {
        'base':    {},
        'detail':  {},
        'weather': {},
    }
    agg = 'day'
    datatype = 'json'
    master_columns = [
        'obs_date',
        'location_geom',
        'dataset_name',
        'weather_observation_id',
        'census_block',
    ]
    weather_columns = [
        'temp_hi',
        'temp_lo',
        'temp_avg',
        'precip_amount',
    ]
    for key, value in params.items():
        if key.split('__')[0] in master_columns:
            queries['base'][key] = value
        elif key.split('__')[0] in weather_columns:
            queries['weather'][key] = value
        elif key == 'agg':
            agg = value
        elif key == 'data_type':
            datatype = value.lower()
        else:
            queries['detail'][key] = value
    return agg, datatype, queries


def make_query(table, raw_query_params):
    table_keys = table.columns.keys()
    args_keys = raw_query_params.keys()
    resp = {
        'meta': {
            'status': 'error',
            'message': '',
        },
        'objects': [],
    }
    status_code = 200
    query_clauses = []
    valid_query = True

    #print "make_query(): args_keys = ", args_keys

    if 'offset' in args_keys:
        args_keys.remove('offset')
    if 'limit' in args_keys:
        args_keys.remove('limit')
    if 'order_by' in args_keys:
        args_keys.remove('order_by')
    if 'weather' in args_keys:
        args_keys.remove('weather')
    for query_param in args_keys:
        try:
            field, operator = query_param.split('__')
            #print "make_query(): field, operator =", field, operator
        except ValueError:
            field = query_param
            operator = 'eq'
        query_value = raw_query_params.get(query_param)
        column = table.columns.get(field)
        if field not in table_keys:
            resp['meta']['message'] = '"%s" is not a valid fieldname' % field
            status_code = 400
            valid_query = False
        elif operator == 'in':
            query = column.in_(query_value.split(','))
            query_clauses.append(query)

        else:
            try:
                # http://stackoverflow.com/questions/14845196/dynamically-constructing-filters-in-sqlalchemy
                attr = filter(
                    lambda e: hasattr(column, e % operator),
                    ['%s', '%s_', '__%s__']
                )[0] % operator
            except IndexError:
                resp['meta']['message'] = '"%s" is not a valid query operator' % operator
                status_code = 400
                valid_query = False
                break
            if query_value == 'null': # pragma: no cover
                query_value = None
            query = getattr(column, attr)(query_value)
            query_clauses.append(query)

    #print "make_query(): query_clauses=", query_clauses
    return valid_query, query_clauses, resp, status_code
