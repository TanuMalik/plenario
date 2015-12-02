from plenario.api.common import cache, crossdomain, CACHE_TIMEOUT, make_cache_key, \
    dthandler, make_csv, extract_first_geometry_fragment
from flask import request, make_response
from dateutil.parser import parse
from datetime import timedelta, datetime
from plenario.models import MetaTable
from itertools import groupby
from operator import itemgetter
import json


VALID_AGG = ['day', 'week', 'month', 'quarter', 'year']


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
