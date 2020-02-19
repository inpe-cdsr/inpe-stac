#!/usr/bin/env python3

"""
STAC API Specification

Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#stac-api-specification
OpenAPI definition: https://stacspec.org/STAC-ext-api.html
"""

from flask import Flask, jsonify, request
from flasgger import Swagger
from werkzeug.exceptions import BadRequest

from inpe_stac.data import get_collections, get_collection_items, \
                            make_json_items, make_json_collection
from inpe_stac.environment import BASE_URI, API_VERSION
from inpe_stac.log import logging
from inpe_stac.decorator import log_function_header, log_function_footer, \
                                catch_generic_exceptions


app = Flask(__name__)

app.config["JSON_SORT_KEYS"] = False
app.config["SWAGGER"] = {
    "openapi": "3.0.1",
    "specs_route": "/docs",
    "title": "INPE STAC Catalog"
}

swagger = Swagger(app, template_file="./spec/api/v0.7/STAC.yaml")


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


##################################################
# OGC API - Features Endpoints
# Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#ogc-api---features-endpoints
##################################################

@app.route("/", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def index():
    links = [
        {"href": f"{BASE_URI}", "rel": "self"},
        {"href": f"{BASE_URI}docs", "rel": "service"},
        {"href": f"{BASE_URI}conformance", "rel": "conformance"},
        {"href": f"{BASE_URI}collections", "rel": "data"},
        {"href": f"{BASE_URI}stac", "rel": "data"},
        {"href": f"{BASE_URI}stac/search", "rel": "search"}
    ]

    return jsonify(links)


@app.route("/conformance", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def conformance():
    conforms = {
        "conformsTo": [
            "http://www.opengis.net/spec/wfs-1/3.0/req/core",
            "http://www.opengis.net/spec/wfs-1/3.0/req/oas30",
            "http://www.opengis.net/spec/wfs-1/3.0/req/html",
            "http://www.opengis.net/spec/wfs-1/3.0/req/geojson"
        ]
    }

    return jsonify(conforms)


@app.route("/collections", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def collections():
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/collection-spec/collection-spec.md#collection-fields
    """

    result = get_collections()

    collections = {
        'collections': []
    }

    for collection in result:
        collections['collections'].append(
            make_json_collection(collection)
        )

    return jsonify(collections)


@app.route("/collections/<collection_id>", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def collections_collections_id(collection_id):
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/collection-spec/collection-spec.md#collection-fields
    """

    result = get_collections(collection_id)

    # if there is not a result, then it returns an empty collection
    if result is None:
        return jsonify({})

    # get the only one element inside the list and create the GeoJSON related to collection
    collection = make_json_collection(result[0])

    return jsonify(collection)


@app.route("/collections/<collection_id>/items", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def collections_collections_id_items(collection_id):
    """
    Example of full route:
        - http://localhost:8089/inpe-stac/collections/CBERS4A_MUX_L2_DN/items?bbox=-68.0273437,-25.0059726,-34.9365234,0.3515602&limit=10000&time=2019-12-22T00:00:00/2020-01-22T23:59:00
    """

    # parameters
    params = {
        'collection_id': collection_id,
        'bbox': request.args.get('bbox', None),
        'time': request.args.get('time', None),
        'intersects': request.args.get('intersects', None),
        'page': int(request.args.get('page', 1)),
        'limit': int(request.args.get('limit', 10)),
        'ids': request.args.get('ids', None)
    }

    items, matched = get_collection_items(**params)

    links = [
        {"href": f"{BASE_URI}collections/", "rel": "self"},
        {"href": f"{BASE_URI}collections/", "rel": "parent"},
        {"href": f"{BASE_URI}collections/", "rel": "collection"},
        {"href": f"{BASE_URI}stac", "rel": "root"}
    ]

    items_collection = make_json_items(items, links)

    items_collection['context'] = {
        "page": params['page'],
        "limit": params['limit'],
        "matched": matched,
        "returned": len(items_collection['features'])
    }

    return jsonify(items_collection)


@app.route("/collections/<collection_id>/items/<item_id>", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def collections_collections_id_items_items_id(collection_id, item_id):
    item, _ = get_collection_items(collection_id=collection_id, item_id=item_id)

    links = [
        {"href": f"{BASE_URI}collections/", "rel": "self"},
        {"href": f"{BASE_URI}collections/", "rel": "parent"},
        {"href": f"{BASE_URI}collections/", "rel": "collection"},
        {"href": f"{BASE_URI}stac", "rel": "root"}
    ]

    gjson = make_json_items(item, links)

    # I'm looking for one item by item_id, ergo just one feature will be returned,
    # then I get this one feature in order to return it
    gjson = gjson['features'][0]

    return jsonify(gjson)


##################################################
# STAC Endpoints
# Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#stac-endpoints
##################################################

@app.route("/stac", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def stac():
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/catalog-spec/catalog-spec.md#catalog-fields
    """

    collections = get_collections()

    catalog = {
        "stac_version": API_VERSION,
        "id": "inpe-stac",
        "description": "INPE STAC Catalog",
        "links": [
            {
                "href": f"{BASE_URI}stac",
                "rel": "self"
            }
        ]
    }

    for collection in collections:
        catalog["links"].append(
            {
                "href": f"{BASE_URI}collections/{collection['id']}",
                "rel": "child",
                "title": collection['id']
            }
        )

    return jsonify(catalog)


@app.route("/stac/search", methods=["GET", "POST"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def stac_search():
    logging.info('stac_search()')

    logging.info('stac_search() - method: %s', request.method)

    if request.method == "POST":
        if request.is_json:
            request_json = request.get_json()

            logging.info('stac_search() - request_json: %s', request_json)

            params = {
                'bbox': request_json.get('bbox', None),
                'time': request_json.get('time', None),
                'ids': request_json.get('ids', None),
                'collections': request_json.get('collections', None),
                'page': int(request_json.get('page', 1)),
                'limit': int(request_json.get('limit', 10))
            }

            if params['bbox'] is not None:
                params['bbox'] = ",".join([str(x) for x in params['bbox']])

            if params['ids'] is not None:
                params['ids'] = ",".join([id for id in params['ids']])
        else:
            raise BadRequest("POST Request must be an application/json")

    elif request.method == "GET":
        logging.info('stac_search() - request.args: %s', request.args)

        params = {
            'bbox': request.args.get('bbox', None),
            'time': request.args.get('time', None),
            'ids': request.args.get('ids', None),
            'collections': request.args.get('collections', None),
            'page': int(request.args.get('page', 1)),
            'limit': int(request.args.get('limit', 10))
        }

    logging.info('stac_search() - params: %s', params)

    items, matched = get_collection_items(**params)

    links = [
        {"href": f"{BASE_URI}collections/", "rel": "self"},
        {"href": f"{BASE_URI}collections/", "rel": "parent"},
        {"href": f"{BASE_URI}collections/", "rel": "collection"},
        {"href": f"{BASE_URI}stac", "rel": "root"}
    ]

    gjson = make_json_items(items, links=links)

    gjson['meta'] = {
        'page': params['page'],
        'limit': params['limit'],
        "matched": matched,
        'returned': len(gjson['features'])
    }

    return jsonify(gjson)


##################################################
# Error Endpoints
##################################################

@app.errorhandler(400)
def handle_bad_request(e):
    resp = jsonify({'code': '400', 'description': 'Bad Request - {}'.format(e.description)})
    resp.status_code = 400

    return resp


@app.errorhandler(404)
def handle_page_not_found(e):
    resp = jsonify({'code': '404', 'description': 'Page not found'})
    resp.status_code = 404

    return resp


@app.errorhandler(500)
def handle_api_error(e):
    resp = jsonify({'code': '500', 'description': 'Internal Server Error'})
    resp.status_code = 500

    return resp


@app.errorhandler(502)
def handle_bad_gateway_error(e):
    resp = jsonify({'code': '502', 'description': 'Bad Gateway'})
    resp.status_code = 502

    return resp


@app.errorhandler(503)
def handle_service_unavailable_error(e):
    resp = jsonify({'code': '503', 'description': 'Service Unavailable'})
    resp.status_code = 503

    return resp


@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.exception(e)
    resp = jsonify({'code': '500', 'description': 'Internal Server Error'})
    resp.status_code = 500

    return resp


##################################################
# Main
##################################################

if __name__ == '__main__':
    app.run()
