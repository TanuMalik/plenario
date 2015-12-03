from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, make_cache_key, \
    dthandler, make_csv, extract_first_geometry_fragment
from flask import request, make_response
from dateutil.parser import parse
from datetime import timedelta, datetime
from plenario.models import MetaTable
from itertools import groupby
from operator import itemgetter
import json
from sqlalchemy import Table
#from collections import defaultdict
from sqlalchemy.exc import NoSuchTableError
from plenario.database import session, Base, app_engine as engine

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
            dataset = Table('dat_' + dataset_name, Base.metadata,
                            autoload_with=engine, extend_existing=True)
            self.cols = dataset.columns.keys()
            # SQLAlchemy 'where' clauses that can be added to a query
            self.filters = []

    def set_optional(self, name, transform, default):
        self.vals[name] = default
        self.transforms[name] = transform

        # For call chaining
        return self

    def validate(self, params):
        for k, v in params.items():
            # Is k a param name with a defined transformation?
            if k in self.transforms.keys():
                val, err = self.transforms[k](v)
                # Was v a valid string for this param name?
                if err:
                    return err
                # Override the default with the transformed value.
                self.vals[k] = val
                continue

            # Is k specifying a filter on a dataset?
            elif self.cols:
                filter = self._make_filter(k, v)
                if filter:
                    self.filters.append(filter)
                    continue

            # This param is neither present in the optional params
            # nor a valid filter.
            warning = 'Unused parameter value "{}={}"'.format(k, v)
            self.warnings.append(warning)

    def _make_filter(self, k, v):
        return None


def agg_validator(agg_str):
    if agg_str in VALID_AGG:
        return agg_str, None
    else:
        error_msg = '{} is not a valid unit of aggregation. Plenario accepts {}'\
                    .format(agg_str, ','.join(VALID_AGG))
        return None, error_msg


def make_format_validator(valid_formats):
    """
    :param valid_formats: A list of strings that are acceptable types of data formats.
    :return: a validator function usable by ParamValidator
    """

    def format_validator(format_str):
        if format_str in valid_formats:
            return format_str, None
        else:
            error_msg = '{} is not a valid output format. Plenario avvepts {}'\
                        .format(format_str, ','.join(valid_formats))
            return error_msg, None

    return format_validator

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

    raw_query_params = request.args.copy()

    validator = ParamValidator().set_optional('agg', agg_validator, 'week')\
                                .set_optional('data_type', )

    # If not set, let agg = day
    agg = raw_query_params.pop('agg', 'day')
    # check for valid temporal aggregate
    if agg not in VALID_AGG:
        resp['meta']['status'] = 'error'
        resp['meta']['message'] = "'%s' is an invalid temporal aggregation" % agg
        resp = make_response(json.dumps(resp, default=dthandler), 400)
        resp.headers['Content-Type'] = 'application/json'
        return resp

    # If not set, let output format be json
    datatype = raw_query_params.pop('data_type', 'json')
    # check for valid output format
    VALID_DATA_TYPE = ['csv', 'json']
    if datatype not in VALID_DATA_TYPE:
        resp['meta']['status'] = 'error'
        resp['meta']['message'] = "'%s' is an invalid output format" % datatype
        resp = make_response(json.dumps(resp, default=dthandler), 400)
        resp.headers['Content-Type'] = 'application/json'
        return resp

    # if no obs_date given, default from 90 days ago...
    try:
        start_date = parse(raw_query_params['obs_date__ge'])
    except KeyError:
        start_date = datetime.now() - timedelta(days=90)
    # ... to today.
    try:
        end_date = parse(raw_query_params['obs_date__le'])
    except KeyError:
        end_date = datetime.now()

    # If dataset names not specifed, look at all point datasets.
    try:
        table_names = raw_query_params['dataset_name__in'].split(',')
    except KeyError:
        table_names = MetaTable.index()

    # If no geom given, don't filter by geography.
    try:
        geojson_doc = raw_query_params['location_geom__within']
        geom = extract_first_geometry_fragment(geojson_doc)
    except KeyError:
        geom = None

    '''
    What do I extract from the query string?
    agg, datatype, start_date, end_date, table_names, geojson_doc
    '''

    # Only examine tables that have a chance of containing records within the date and space boundaries.
    table_names = MetaTable.narrow_candidates(table_names, start_date, end_date, geom)

    panel = MetaTable.timeseries_all(table_names=table_names,
                                     agg_unit=agg,
                                     start=start_date,
                                     end=end_date,
                                     geom=geom)

    resp['objects'] = panel
    resp['meta']['query'] = raw_query_params
    if geom:
        resp['meta']['query']['location_geom__within'] = geom
    resp['meta']['query']['agg'] = agg
    resp['meta']['status'] = 'ok'

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
    raw_query_params = request.args.copy()
    # if no obs_date given, default to >= 30 days ago
    obs_dates = [i for i in raw_query_params.keys() if i.startswith('obs_date')]
    if not obs_dates:
        six_months_ago = datetime.now() - timedelta(days=30)
        raw_query_params['obs_date__ge'] = six_months_ago.strftime('%Y-%m-%d')

    # include_weather = False
    '''if raw_query_params.get('weather') is not None:
        include_weather = raw_query_params['weather']
        del raw_query_params['weather']'''
    agg, datatype, queries = parse_join_query(raw_query_params)
    offset = raw_query_params.get('offset')
    mt = MasterTable.__table__
    valid_query, base_clauses, resp, status_code = make_query(mt, queries['base'])
    if not raw_query_params.get('dataset_name'):
        valid_query = False
        resp['meta'] = {
            'status': 'error',
            'message': "'dataset_name' is required"
        }
        resp['objects'] = []
    if valid_query:
        resp['meta']['status'] = 'ok'
        dname = raw_query_params['dataset_name']
        dataset = Table('dat_%s' % dname, Base.metadata,
            autoload=True, autoload_with=engine,
            extend_existing=True)
        dataset_fields = dataset.columns.keys()
        base_query = session.query(mt, dataset)
        '''if include_weather:
            date_col_name = 'date'
            try:
                date_col_name = slugify(session.query(MetaTable)\
                    .filter(MetaTable.dataset_name == dname)\
                    .first().observed_date)
            except AttributeError:
                pass
            date_col_type = str(getattr(dataset.c, date_col_name).type).lower()
            if 'timestamp' in date_col_type:
                weather_tname = 'hourly'
            else:
                weather_tname = 'daily'
            weather_table = Table('dat_weather_observations_%s' % weather_tname, Base.metadata,
                autoload=True, autoload_with=engine, extend_existing=True)
            weather_fields = weather_table.columns.keys()
            base_query = session.query(mt, dataset, weather_table)'''
        valid_query, detail_clauses, resp, status_code = make_query(dataset, queries['detail'])
        if valid_query:
            resp['meta']['status'] = 'ok'
            pk = [p.name for p in dataset.primary_key][0]
            base_query = base_query.join(dataset, mt.c.dataset_row_id == dataset.c[pk])
            for clause in base_clauses:
                base_query = base_query.filter(clause)
            for clause in detail_clauses:
                base_query = base_query.filter(clause)

            # Ignoring weather for the moment
            '''
            if include_weather:
                w_q = {}
                if queries['weather']:
                    for k,v in queries['weather'].items():
                        try:
                            fname, operator = k.split('__')
                        except ValueError:
                            operator = 'eq'
                            pass
                        t_fname = WEATHER_COL_LOOKUP[weather_tname].get(fname, fname)
                        w_q['__'.join([t_fname, operator])] = v
                valid_query, weather_clauses, resp, status_code = make_query(weather_table, w_q)
                if valid_query:
                    resp['meta']['status'] = 'ok'
                    base_query = base_query.join(weather_table, mt.c.weather_observation_id == weather_table.c.id)
                    for clause in weather_clauses:
                        base_query = base_query.filter(clause)'''
            if valid_query:
                base_query = base_query.limit(RESPONSE_LIMIT)
                if offset:
                    base_query = base_query.offset(int(offset))
                values = [r for r in base_query.all()]
                for value in values:
                    d = {f:getattr(value, f) for f in dataset_fields}
                    if value.location_geom is not None:
                        d['location_geom'] = loads(value.location_geom.desc, hex=True).__geo_interface__
                    '''if include_weather:
                        d = {
                            'observation': {f:getattr(value, f) for f in dataset_fields},
                            'weather': {f:getattr(value, f) for f in weather_fields},
                        }'''
                    resp['objects'].append(d)
                resp['meta']['query'] = raw_query_params
                loc = resp['meta']['query'].get('location_geom__within')
                if loc:
                    resp['meta']['query']['location_geom__within'] = json.loads(loc)
                resp['meta']['total'] = len(resp['objects'])
    if datatype == 'json':
        resp = make_response(json.dumps(resp, default=dthandler), status_code)
        resp.headers['Content-Type'] = 'application/json'

    elif datatype == 'geojson': #and not include_weather:
        geojson_resp = {
          "type": "FeatureCollection",
          "features": []
        }

        for o in resp['objects']:
            if o.get('location_geom'):
                g = {
                  "type": "Feature",
                  "geometry": o['location_geom'],
                  "properties": {f:getattr(value, f) for f in o}
                }
                geojson_resp['features'].append(g)

        resp = make_response(json.dumps(geojson_resp, default=dthandler), status_code)
        resp.headers['Content-Type'] = 'application/json'
    elif datatype == 'csv':
        csv_resp = [dataset_fields]
        '''if include_weather:
            csv_resp = [dataset_fields + weather_fields]'''
        for value in values:
            d = [getattr(value, f) for f in dataset_fields]
            '''if include_weather:
                d.extend([getattr(value, f) for f in weather_fields])'''
            csv_resp.append(d)
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
        elif operator == 'within':
            geo = json.loads(query_value)
            #print "make_query(): geo is", geo.items()
            if 'features' in geo.keys():
                val = geo['features'][0]['geometry']
            elif 'geometry' in geo.keys():
                val = geo['geometry']
            else:
                val = geo
            if val['type'] == 'LineString':
                shape = asShape(val)
                lat = shape.centroid.y
                # 100 meters by default
                x, y = getSizeInDegrees(100, lat)
                val = shape.buffer(y).__geo_interface__
            val['crs'] = {"type":"name","properties":{"name":"EPSG:4326"}}
            query = column.ST_Within(func.ST_GeomFromGeoJSON(json.dumps(val)))
            #print "make_query: val=", val
            #print "make_query(): query = ", query
            query_clauses.append(query)
        elif operator.startswith('time_of_day'):
            if operator.endswith('ge'):
                query = func.date_part('hour', column).__ge__(query_value)
            elif operator.endswith('le'):
                query = func.date_part('hour', column).__le__(query_value)
            query_clauses.append(query)
        else:
            try:
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

"""
I need a sane way to declare what arguments are required, set defaults, etc.

What kind of params are there?

Enum-like: agg, datatype are strings that must be contained in a predefined list
Time: dates
Geom: a geojson string
string: dataset_name
kwarg: dataset_field = name
int: resolution, buffer,


"""

'''
Usecase: /timeseries
What do I extract from the query string?
agg, datatype, start_date, end_date, table_names, geojson_doc

ParamParser().set_optional('dataset_names__in', None)
             .set_optional('obs_date__ge', date_param, 90_ago)
             .set_optional('obs_date__le', date_param, today)
             .set_optional('agg', agg_enum, 'day')
             .set_optional('location_geom__within', geojson_param, None)

Usecase: /detail

ParamParser().set_optional('')

'''


class Enum(object):

    def __init__(self, name, valid_vals):
        self.name = name
        self.valid_vals = valid_vals


class Param(object):
    def __init__(self, name, is_valid, transform=None):
        self.name = name
        self._is_valid = is_valid
        self.transform = transform

    def make(self, key, val):
        if not self._is_valid(key, val):
            return None

        if self.transform:
            val = self.transform(val)
        return val



