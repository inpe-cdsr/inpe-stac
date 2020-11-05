#!/usr/bin/env python3

"""
STAC API Specification

Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#stac-api-specification
OpenAPI definition: https://stacspec.org/STAC-ext-api.html
"""

from flask import Flask, jsonify, request
from flasgger import Swagger
from werkzeug.exceptions import BadRequest

from inpe_stac.data import get_collections, get_collection_items, make_json_items, \
                           make_json_collection, make_json_item_collection
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

swagger = Swagger(app, template_file="./spec/api/v0.9.0/STAC.yaml")


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


##################################################
# OGC API - Features Endpoints
# Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/api-spec.md#ogc-api---features-endpoints
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
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/collection-spec/collection-spec.md#collection-fields
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
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/collection-spec/collection-spec.md#collection-fields
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
    Specifications:
        - https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/api-spec.md#filter-parameters-and-fields
    """

    logging.info('collections_collections_id_items')

    # parameters
    params = {
        'collection_id': collection_id,
        'bbox': request.args.get('bbox', None),
        'time': request.args.get('time', None),
        'ids': request.args.get('ids', None),
        'intersects': request.args.get('intersects', None),  # not implemented yet
        'page': int(request.args.get('page', 1)),
        'limit': int(request.args.get('limit', 10))
    }

    logging.info(f'collections_collections_id_items - params: {params}')

    items, matched, _ = get_collection_items(**params)

    # links to each Item inside ItemCollection
    links = [
        {"href": f"{BASE_URI}collections/", "rel": "self"},
        {"href": f"{BASE_URI}collections/", "rel": "parent"},
        {"href": f"{BASE_URI}collections/", "rel": "collection"},
        {"href": f"{BASE_URI}stac", "rel": "root"}
    ]

    item_collection = make_json_items(
        items, links, item_stac_extensions=['eo']
    )

    item_collection = make_json_item_collection(item_collection, params, matched)

    # remove unnecessary property to build the URL below
    del params['collection_id']
    # convert 'params' from dict to str to add to the URL
    params = '&'.join([f'{k}={v}' for k, v in params.items() if v is not None])

    # links to this ItemCollection
    item_collection['links'] = [
        {"href": f"{BASE_URI}collections/{collection_id}/items?{params}", "rel": "self"},
        {"href": f"{BASE_URI}collections/{collection_id}/items", "rel": "parent"},
        {"href": f"{BASE_URI}collections/{collection_id}", "rel": "parent"},
        {"href": f"{BASE_URI}collections", "rel": "collection"},
        {"href": f"{BASE_URI}stac", "rel": "root"}
    ]

    return jsonify(item_collection)


@app.route("/collections/<collection_id>/items/<item_id>", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def collections_collections_id_items_items_id(collection_id, item_id):
    logging.info('collections_collections_id_items_items_id')

    logging.info(f'collections_collections_id_items_items_id - collection_id: {collection_id}')
    logging.info(f'collections_collections_id_items_items_id - item_id: {item_id}')

    item, _, _ = get_collection_items(collection_id=collection_id, item_id=item_id)

    links = [
        {"href": f"{BASE_URI}collections/", "rel": "self"},
        {"href": f"{BASE_URI}collections/", "rel": "parent"},
        {"href": f"{BASE_URI}collections/", "rel": "collection"},
        {"href": f"{BASE_URI}stac", "rel": "root"}
    ]

    item_collection = make_json_items(
        item, links, item_stac_extensions=['eo']
    )

    # if an item was not returned, then I return an empty item
    item = {}

    # else, if an item was returned, then I return the item
    if item_collection['features']:
        # I'm looking for one item by item_id, ergo just one feature will be returned,
        # then I get this one feature in order to return it
        item = item_collection['features'][0]

    return jsonify(item)


##################################################
# STAC Endpoints
# Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/api-spec.md#stac-endpoints
##################################################

@app.route("/stac", methods=["GET"])
@log_function_header
@log_function_footer
@catch_generic_exceptions
def stac():
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/catalog-spec/catalog-spec.md#catalog-fields
    """

    collections = get_collections()

    catalog = {
        "stac_version": API_VERSION,
        'stac_extensions': [],
        "id": "inpe-stac",
        "description": "INPE STAC Catalog",
        "links": [
            {
                "href": f"{BASE_URI}stac",
                "rel": "self"
            },
            {
                "href": f"{BASE_URI}collections",
                "rel": "collections"
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
    """
    Specifications:
        - https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/api-spec.md#filter-parameters-and-fields
        - https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/extensions/query/README.md
    """

    logging.info('stac_search')

    logging.info(f'stac_search - method: {request.method}')

    if request.method == "POST":
        if request.is_json:
            request_json = request.get_json()

            logging.info(f'stac_search - request_json: {request_json}')

            params = {
                'bbox': request_json.get('bbox', None),
                'time': request_json.get('time', None),
                'ids': request_json.get('ids', None),
                'collections': request_json.get('collections', None),
                'page': int(request_json.get('page', 1)),
                'limit': int(request_json.get('limit', 10)),
                'query': request_json.get('query', None)
            }

            if params['bbox'] is not None:
                params['bbox'] = ','.join([str(x) for x in params['bbox']])

            if params['ids'] is not None:
                params['ids'] = ','.join(params['ids'])

            # if params['collections'] is not None:
            #     params['collections'] = ','.join([collection for collection in params['collections']])
        else:
            raise BadRequest('POST Request must be an application/json')

    elif request.method == 'GET':
        logging.info('stac_search() - request.args: %s', request.args)

        params = {
            'bbox': request.args.get('bbox', None),
            'time': request.args.get('time', None),
            'ids': request.args.get('ids', None),
            'collections': request.args.get('collections', None),
            'page': int(request.args.get('page', 1)),
            'limit': int(request.args.get('limit', 10))
        }

        if isinstance(params['collections'], str):
            params['collections'] = params['collections'].split(',')

    logging.info(f'stac_search() - params: {params}')

    items, matched, metadata_related_to_collections = get_collection_items(**params)

    links = [
        {'href': f'{BASE_URI}collections/', 'rel': 'self'},
        {'href': f'{BASE_URI}collections/', 'rel': 'parent'},
        {'href': f'{BASE_URI}collections/', 'rel': 'collection'},
        {'href': f'{BASE_URI}stac', 'rel': 'root'}
    ]

    item_collection = make_json_items(
        items, links, item_stac_extensions=['eo', 'query']
    )

    item_collection = make_json_item_collection(
        item_collection, params, matched, meta=metadata_related_to_collections
    )

    # # remove unnecessary property to build the URL below
    # del params['collection_id']
    # # convert 'params' from dict to str to add to the URL
    # params = '&'.join([f'{k}={v}' for k, v in params.items() if v is not None])

    # # links to this ItemCollection
    # item_collection['links'] = [
    #     {"href": f"{BASE_URI}collections/{collection_id}/items?{params}", "rel": "self"},
    #     {"href": f"{BASE_URI}collections/{collection_id}/items", "rel": "parent"},
    #     {"href": f"{BASE_URI}collections/{collection_id}", "rel": "parent"},
    #     {"href": f"{BASE_URI}collections", "rel": "collection"},
    #     {"href": f"{BASE_URI}stac", "rel": "root"}
    # ]

    return jsonify(item_collection)


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
