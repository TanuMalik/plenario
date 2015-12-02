import json
from flask import make_response, Blueprint
from point import timeseries
from common import cache
from shape import get_all_shape_datasets, find_intersecting_shapes, export_shape

API_VERSION = '/v1'

api = Blueprint('api', __name__)
prefix = API_VERSION + '/api'
api.add_url_rule(prefix + '/timeseries', 'timeseries', timeseries)
api.add_url_rule(prefix + '/shapes/', 'shape_index', get_all_shape_datasets)
api.add_url_rule(prefix + '/shapes/intersections/<geojson>', 'shape_intersections', find_intersecting_shapes)
api.add_url_rule(prefix + '/shapes/<dataset_name>', 'shape_export', export_shape)


@api.route(API_VERSION + '/api/flush-cache')
def flush_cache():
    cache.clear()
    resp = make_response(json.dumps({'status': 'ok', 'message': 'cache flushed!'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp
