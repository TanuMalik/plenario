import json
from flask import make_response, Blueprint
from point import timeseries
from common import cache

API_VERSION = '/v1'

api = Blueprint('api', __name__)
prefix = API_VERSION + '/api'
api.add_url_rule(prefix + '/timeseries', 'timeseries', timeseries)


@api.route(API_VERSION + '/api/flush-cache')
def flush_cache():
    cache.clear()
    resp = make_response(json.dumps({'status': 'ok', 'message': 'cache flushed!'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp
