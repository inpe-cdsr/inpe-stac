
from os import getenv
from functools import reduce
from json import loads
from pprint import PrettyPrinter

from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy.sql import text
from time import time
from werkzeug.exceptions import BadRequest, InternalServerError

from inpe_stac.log import logging
from inpe_stac.decorator import log_function_header
from inpe_stac.environment import API_VERSION, BASE_URI, INPE_STAC_DELETED


pp = PrettyPrinter(indent=4)


def len_result(result):
    return len(result) if result is not None else len([])


def insert_deleted_flag_to_where(where):
    if INPE_STAC_DELETED == '0':
        where.insert(0, 'deleted = 0')
    elif INPE_STAC_DELETED == '1':
        where.insert(0, 'deleted = 1')
    else:
        # if INPE_STAC_DELETED flag is another string,
        # then I don't insert this flag on the search,
        # in other words, I search all scenes
        pass


@log_function_header
def get_collections(collection_id=None):
    logging.info('get_collections - collection_id: {}'.format(collection_id))

    kwargs = {}
    where = ''

    # if there is a 'collection_id' key to search, then add the WHERE clause and the key to kwargs
    if collection_id is not None:
        where = 'WHERE id = :collection_id'
        kwargs = { 'collection_id': collection_id }

    query = 'SELECT * FROM stac_collection {};'.format(where)

    logging.info('get_collections - query: {}'.format(query))

    result, elapsed_time = do_query(query, **kwargs)

    logging.info('get_collections - elapsed_time - query: {}'.format(timedelta(seconds=elapsed_time)))

    logging.info('get_collections - len(result): {}'.format(len_result(result)))
    # logging.debug('get_collections - result: {}'.format(result))

    return result


@log_function_header
def __search_stac_item_view(where, params):
    logging.info('__search_stac_item_view')

    insert_deleted_flag_to_where(where)

    # create the WHERE clause
    where = '\nAND '.join(where)

    # if the user is looking for more than one collection, then I search by partition
    if 'collections' in params:
        sql = '''
            SELECT *
            FROM (
                SELECT *, row_number() over (partition by collection) rn
                FROM stac_item
                WHERE
                    {}
            ) t
            WHERE rn >= :page AND rn <= :limit;
        '''.format(where)
    # else, I search with a normal query
    else:
        sql = '''
            SELECT *
            FROM stac_item
            WHERE
                {}
            LIMIT :page, :limit
        '''.format(where)

    # add just where clause to query, because I want to get the number of total results
    sql_count = '''
        SELECT collection, COUNT(id) as matched
        FROM stac_item
        WHERE
            {}
        GROUP BY collection;
    '''.format(where)

    # logging.info('__search_stac_item_view - where: {}'.format(where))
    logging.info('__search_stac_item_view - params: {}'.format(params))

    logging.info('__search_stac_item_view - sql_count: {}'.format(sql_count))
    logging.info('__search_stac_item_view - sql: {}'.format(sql))

    # execute the queries
    result_count, elapsed_time = do_query(sql_count, **params)
    logging.info('__search_stac_item_view - elapsed_time - sql_count: {}'.format(timedelta(seconds=elapsed_time)))

    result, elapsed_time = do_query(sql, **params)
    logging.info('__search_stac_item_view - elapsed_time - sql: {}'.format(timedelta(seconds=elapsed_time)))

    # if `result` or `result_count` is None, then I return an empty list instead
    if result is None:
        result = []

    if result_count is None:
        result_count = []

    if 'collections' in params:
        for collection in params['collections'].split(','):
            if not any(d['collection'] == collection for d in result_count):
                result_count.append(
                    {'collection': collection, 'matched': 0}
                )

        result_count = sorted(result_count, key=lambda key: key['collection'])

    # logging.debug('__search_stac_item_view - result: \n{}\n'.format(result))
    logging.info('__search_stac_item_view - returned: {}'.format(len_result(result)))
    logging.info('__search_stac_item_view - result_count: \n{}\n'.format(result_count))

    return result, result_count


@log_function_header
def get_collection_items(collection_id=None, item_id=None, bbox=None, time=None,
                         intersects=None, page=1, limit=10, ids=None, collections=None,
                         query=None):
    logging.info('get_collection_items()')

    result = []
    metadata_related_to_collections = []
    matched = 0

    params = {
        'page': page - 1,
        'limit': limit
    }

    default_where = []

    # search for ids
    if item_id is not None or ids is not None:
        if item_id is not None:
            default_where.append('id = :item_id')
            params['item_id'] = item_id
        elif ids is not None:
            default_where.append('FIND_IN_SET(id, :ids)')
            params['ids'] = ','.join(ids)

        logging.info('get_collection_items() - default_where: {}'.format(default_where))

        __result, __matched = __search_stac_item_view(default_where, params)

        result += __result
        matched += reduce(lambda x, y: x + y['matched'], __matched, 0) if __matched else 0

    else:
        if bbox is not None:
            try:
                for x in bbox.split(','):
                    float(x)

                params['min_x'], params['min_y'], params['max_x'], params['max_y'] = bbox.split(',')

                # replace method removes extra espace caused by multi-line String
                default_where.append(
                    '''(
                    ((:min_x <= tr_longitude and :min_y <= tr_latitude)
                    or
                    (:min_x <= br_longitude and :min_y <= tl_latitude))
                    and
                    ((:max_x >= bl_longitude and :max_y >= bl_latitude)
                    or
                    (:max_x >= tl_longitude and :max_y >= br_latitude))
                    )'''.replace('                ', '')
                )
            except:
                raise (InvalidBoundingBoxError())

        if time is not None:
            if not (isinstance(time, str) or isinstance(time, list)):
                raise BadRequest('`time` field is not a string or list')

            # if time is a string, then I convert it to list by splitting it
            if isinstance(time, str):
                time = time.split("/")

            # if there is time_start and time_end, then get them
            if len(time) == 2:
                params['time_start'], params['time_end'] = time
                default_where.append("date <= :time_end")
            # if there is just time_start, then get it
            elif len(time) == 1:
                params['time_start'] = time[0]

            default_where.append("date >= :time_start")

        logging.info('get_collection_items() - default_where: {}'.format(default_where))

        # if query is a dict, then get all available fields to search
        # Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/api-spec/extensions/query/README.md
        if isinstance(query, dict):
            for field, value in query.items():
                # eq, neq, lt, lte, gt, gte
                if 'eq' in value:
                    default_where.append('{0} = {1}'.format(field, value['eq']))
                if 'neq' in value:
                    default_where.append('{0} != {1}'.format(field, value['neq']))
                if 'lt' in value:
                    default_where.append('{0} < {1}'.format(field, value['lt']))
                if 'lte' in value:
                    default_where.append('{0} <= {1}'.format(field, value['lte']))
                if 'gt' in value:
                    default_where.append('{0} > {1}'.format(field, value['gt']))
                if 'gte' in value:
                    default_where.append('{0} >= {1}'.format(field, value['gte']))
                # startsWith, endsWith, contains
                if 'startsWith' in value:
                    default_where.append('{0} LIKE \'{1}%\''.format(field, value['startsWith']))
                if 'endsWith' in value:
                    default_where.append('{0} LIKE \'%{1}\''.format(field, value['endsWith']))
                if 'contains' in value:
                    default_where.append('{0} LIKE \'%{1}%\''.format(field, value['contains']))

        if collection_id is not None and isinstance(collection_id, str):
            collections = [collection_id]

        # search for collections
        if collections is not None:
            logging.info('get_collection_items() - collections: {}'.format(collections))

            # append the query at the beginning of the list
            default_where.insert(0, 'FIND_IN_SET(collection, :collections)')
            params['collections'] = ','.join(collections)

            __result, __matched = __search_stac_item_view(default_where, params)

            result += __result
            # sum all `matched` keys from the `__matched` list. initialize the first `x` with `0`
            # source: https://stackoverflow.com/a/42453184
            matched += reduce(lambda x, y: x + y['matched'], __matched, 0) if __matched else 0

            metadata_related_to_collections = [
                {
                    'name': d['collection'],
                    'context': {
                        'page': page,
                        'limit': limit,
                        'matched': d['matched'],
                        # count just the results related to the selected collection
                        'returned': len(list(filter(
                            lambda x: x['collection'] == d['collection'],
                            result
                        )))
                    }
                # d - dictionary
                } for d in __matched
            ]

        # search for anything else
        else:
            __result, __matched = __search_stac_item_view(default_where, params)

            result += __result
            matched += reduce(lambda x, y: x + y['matched'], __matched, 0) if __matched else 0

    logging.info('get_collection_items() - matched: {}'.format(matched))
    # logging.debug('get_collection_items() - result: \n\n{}\n\n'.format(result))
    logging.debug('get_collection_items() - metadata: {}'.format(metadata_related_to_collections))

    return result, matched, metadata_related_to_collections


def make_json_collection(collection_result):
    collection_id = collection_result['id']

    start_date = collection_result['start_date'].isoformat()
    end_date = None if collection_result['end_date'] is None else collection_result['end_date'].isoformat()

    collection = {
        'stac_version': API_VERSION,
        'id': collection_id,
        'title': collection_id,
        'description': collection_result['description'],
        'license': None,
        'extent': {
            'spatial': [
                collection_result['min_x'], collection_result['min_y'],
                collection_result['max_x'], collection_result['max_y']
            ],
            'temporal': [ start_date, end_date ]
        },
        'properties': {},
        'links': [
            {'href': f'{BASE_URI}collections/{collection_id}', 'rel': 'self'},
            {'href': f'{BASE_URI}collections/{collection_id}/items', 'rel': 'items'},
            {'href': f'{BASE_URI}collections', 'rel': 'parent'},
            {'href': f'{BASE_URI}collections', 'rel': 'root'},
            {'href': f'{BASE_URI}stac', 'rel': 'root'}
        ]
    }

    return collection


def make_json_items(items, links):
    # logging.debug('make_geojson - items: {}'.format(items))
    # logging.debug('make_geojson - links: {}'.format(links))

    if items is None:
        return {
            'type': 'FeatureCollection',
            'features': []
        }

    features = []

    gjson = OrderedDict()
    gjson['type'] = 'FeatureCollection'

    if len(items) == 0:
        gjson['features'] = features
        return gjson

    for i in items:
        # logging.info('make_json_items - id: %s', i['id'])
        # logging.info('make_json_items - item:')
        # pp.pprint(i)
        # print('\n\n')

        feature = OrderedDict()

        feature['type'] = 'Feature'
        feature['id'] = i['id']
        feature['collection'] = i['collection']

        geometry = dict()
        geometry['type'] = 'Polygon'
        geometry['coordinates'] = [
          [[i['tl_longitude'], i['tl_latitude']],
           [i['bl_longitude'], i['bl_latitude']],
           [i['br_longitude'], i['br_latitude']],
           [i['tr_longitude'], i['tr_latitude']],
           [i['tl_longitude'], i['tl_latitude']]]
        ]
        feature['geometry'] = geometry
        feature['bbox'] = bbox(feature['geometry']['coordinates'])

        feature['properties'] = {
            # format the datetime
            'datetime': datetime.fromisoformat(str(i['datetime'] )).isoformat(),
            'path': i['path'],
            'row': i['row'],
            'satellite': i['satellite'],
            'sensor': i['sensor'],
            'cloud_cover': i['cloud_cover'],
            'sync_loss': i['sync_loss']
        }

        feature['assets'] = {}

        # convert string json to dict json
        i['assets'] = loads(i['assets'])

        for asset in i['assets']:
            feature['assets'][asset['band']] = {
                'href': getenv('TIF_ROOT') + asset['href'],
                'type': 'image/vnd.stac.geotiff'
            }
            feature['assets'][asset['band'] + '_xml'] = {
                'href': getenv('TIF_ROOT') + asset['href'].replace('.tif', '.xml'),
                'type': 'text/xml'
            }

        feature['assets']['thumbnail'] = {
            'href': getenv('PNG_ROOT') + i['thumbnail'],
            'type': 'image/png'
        }

        feature['links'] = deepcopy(links)
        feature['links'][0]['href'] += i['collection'] + "/items/" + i['id']
        feature['links'][1]['href'] += i['collection']
        feature['links'][2]['href'] += i['collection']

        features.append(feature)

        # print('\nfeature: ')
        # pp.pprint(feature)
        # print('\n')

    gjson['features'] = features

    # logging.debug('make_geojson - gjson: {}'.format(gjson))

    return gjson


def do_query(sql, **kwargs):
    start_time = time()

    connection = 'mysql://{}:{}@{}/{}'.format(
        getenv('DB_USER'), getenv('DB_PASS'), getenv('DB_HOST'), getenv('DB_NAME')
    )
    engine = sqlalchemy.create_engine(connection)

    sql = text(sql)
    engine.execute("SET @@group_concat_max_len = 1000000;")

    result = engine.execute(sql, kwargs)
    result = result.fetchall()

    engine.dispose()

    result = [ dict(row) for row in result ]

    elapsed_time = time() - start_time

    if len(result) > 0:
        return result, elapsed_time
    else:
        return None, elapsed_time


def bbox(coord_list):
    box = []

    for i in (0, 1):
        res = sorted(coord_list[0], key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))

    return [box[0][0], box[1][0], box[0][1], box[1][1]]


class InvalidBoundingBoxError(Exception):
    pass
