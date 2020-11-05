
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
from inpe_stac.environment import API_VERSION, BASE_URI, INPE_STAC_DELETED, \
                                  DB_USER, DB_PASS, DB_HOST, DB_NAME


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
    logging.info('get_collections')
    logging.info(f'get_collections - collection_id: {collection_id}')

    kwargs = {}
    sc_where = si_where = ''

    # if there is a 'collection_id' key to search, then add the WHERE clause and the key to kwargs
    if collection_id is not None:
        sc_where = 'WHERE id = :collection_id'
        si_where = 'WHERE collection = :collection_id'
        kwargs = { 'collection_id': collection_id }

    query = f'''
        SELECT *
        FROM stac_collection sc
        LEFT JOIN (
            SELECT collection, assets
            FROM `stac_item`
            {si_where}
            GROUP BY collection
        ) si
        ON sc.id = si.collection
        {sc_where};
    '''

    logging.info(f'get_collections - query: {query}')

    result, elapsed_time = do_query(query, **kwargs)

    logging.info(f'get_collections - elapsed_time - query: {timedelta(seconds=elapsed_time)}')

    logging.info(f'get_collections - len(result): {len_result(result)}')
    # logging.debug(f'get_collections - result: {result}')

    return result


@log_function_header
def __search_stac_item_view(where, params):
    logging.info('__search_stac_item_view')

    insert_deleted_flag_to_where(where)

    # create the WHERE clause
    where = '\nAND '.join(where)

    # if the user is looking for more than one collection, then I search by partition
    if 'collections' in params:
        sql = f'''
            SELECT *
            FROM (
                SELECT *, row_number() over (partition by collection) rn
                FROM stac_item
                WHERE
                    {where}
            ) t
            WHERE rn >= :page AND rn <= :limit;
        '''
    # else, I search with a normal query
    else:
        sql = f'''
            SELECT *
            FROM stac_item
            WHERE
                {where}
            LIMIT :page, :limit
        '''

    # add just where clause to query, because I want to get the number of total results
    sql_count = f'''
        SELECT collection, COUNT(id) as matched
        FROM stac_item
        WHERE
            {where}
        GROUP BY collection;
    '''

    # logging.info(f'__search_stac_item_view - where: {where}')
    logging.info(f'__search_stac_item_view - params: {params}')

    logging.info(f'__search_stac_item_view - sql_count: {sql_count}')
    logging.info(f'__search_stac_item_view - sql: {sql}')

    # execute the queries
    result_count, elapsed_time = do_query(sql_count, **params)
    logging.info(f'__search_stac_item_view - elapsed_time - sql_count: {timedelta(seconds=elapsed_time)}')

    result, elapsed_time = do_query(sql, **params)
    logging.info(f'__search_stac_item_view - elapsed_time - sql: {timedelta(seconds=elapsed_time)}')

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

    # logging.debug(f'__search_stac_item_view - result: \n{result}\n')
    logging.info(f'__search_stac_item_view - returned: {len_result(result)}')
    logging.info(f'__search_stac_item_view - result_count: {result_count}')

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
            params['ids'] = ids

        logging.info(f'get_collection_items() - default_where: {default_where}')

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
                time = time.split('/')

            # if there is time_start and time_end, then get them
            if len(time) == 2:
                params['time_start'], params['time_end'] = time
                default_where.append('date <= :time_end')
            # if there is just time_start, then get it
            elif len(time) == 1:
                params['time_start'] = time[0]

            default_where.append('date >= :time_start')

        logging.info(f'get_collection_items() - default_where: {default_where}')

        # if query is a dict, then get all available fields to search
        # Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/extensions/query/README.md
        if isinstance(query, dict):
            for field, value in query.items():
                # eq, neq, lt, lte, gt, gte
                if 'eq' in value:
                    default_where.append(f'{field} = {value["eq"]}')
                if 'neq' in value:
                    default_where.append(f'{field} != {value["neq"]}')
                if 'lt' in value:
                    default_where.append(f'{field} < {value["lt"]}')
                if 'lte' in value:
                    default_where.append(f'{field} <= {value["lte"]}')
                if 'gt' in value:
                    default_where.append(f'{field} > {value["gt"]}')
                if 'gte' in value:
                    default_where.append(f'{field} >= {value["gte"]}')
                # startsWith, endsWith, contains
                if 'startsWith' in value:
                    default_where.append(f'{field} LIKE \'{value["startsWith"]}%\'')
                if 'endsWith' in value:
                    default_where.append(f'{field} LIKE \'%{value["endsWith"]}\'')
                if 'contains' in value:
                    default_where.append(f'{field} LIKE \'%{value["contains"]}%\'')

        if collection_id is not None and isinstance(collection_id, str):
            collections = [collection_id]

        # search for collections
        if collections is not None:
            logging.info(f'get_collection_items() - collections: {collections}')

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

    logging.info(f'get_collection_items() - matched: {matched}')
    # logging.debug(f'get_collection_items() - result: \n{result}\n')
    logging.debug(f'get_collection_items() - metadata: {metadata_related_to_collections}')

    return result, matched, metadata_related_to_collections


def make_json_collection(collection_result):
    collection_id = collection_result['id']

    start_date = collection_result['start_date'].isoformat()
    end_date = None if collection_result['end_date'] is None else collection_result['end_date'].isoformat()

    eo_bands = []

    # collection_result["assets"] is a string, then I convert it to a list with dictionaries
    assets = loads(collection_result["assets"])

    # I create the 'eo:bands' property based on 'assets'
    for asset in assets:
        eo_bands.append(
            {
                'name': asset['band'],
                'common_name': asset['band']
            }
        )

    collection = {
        'stac_version': API_VERSION,
        'stac_extensions': ['eo'],
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
        'properties': {
            'eo:bands': eo_bands
        },
        'links': [
            {'href': f'{BASE_URI}collections/{collection_id}', 'rel': 'self'},
            {'href': f'{BASE_URI}collections/{collection_id}/items', 'rel': 'items'},
            {'href': f'{BASE_URI}collections', 'rel': 'parent'},
            {'href': f'{BASE_URI}collections', 'rel': 'root'},
            {'href': f'{BASE_URI}stac', 'rel': 'root'}
        ]
    }

    return collection


def make_json_items(items, links, item_stac_extensions=None):
    # logging.debug(f'make_geojson - items: {items}')
    # logging.debug(f'make_geojson - links: {links}')

    if items is None:
        return {
            'type': 'FeatureCollection',
            'features': []
        }

    features = []

    gjson = OrderedDict()
    gjson['stac_version'] = API_VERSION
    gjson['stac_extensions'] = []
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

        feature['stac_version'] = API_VERSION
        feature['stac_extensions'] = item_stac_extensions
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

        ##################################################
        # properties
        ##################################################

        feature['properties'] = {
            # format the datetime
            'datetime': datetime.fromisoformat(str(i['datetime'] )).isoformat(),
            'path': i['path'],
            'row': i['row'],
            'satellite': i['satellite'],
            'sensor': i['sensor'],
            'cloud_cover': i['cloud_cover'],
            'sync_loss': i['sync_loss'],
            'eo:gsd': -1
            # 'eo:bands' is going to be added below
        }

        ##################################################
        # assets
        ##################################################

        feature['assets'] = {}
        eo_bands = []

        # convert string json to dict json
        i['assets'] = loads(i['assets'])

        for asset in i['assets']:
            eo_bands.append(
                {
                    'name': asset['band'],
                    'common_name': asset['band']
                }
            )

            feature['assets'][asset['band']] = {
                'href': getenv('TIF_ROOT') + asset['href'],
                'type': 'image/tiff; application=geotiff',
                # get index of the last added item
                'eo:bands': [len(eo_bands) - 1]
            }
            feature['assets'][asset['band'] + '_xml'] = {
                'href': getenv('TIF_ROOT') + asset['href'].replace('.tif', '.xml'),
                'type': 'application/xml'
            }

        feature['assets']['thumbnail'] = {
            'href': getenv('PNG_ROOT') + i['thumbnail'],
            'type': 'image/png'
        }

        # add eo:bands to properties
        feature['properties']['eo:bands'] = eo_bands

        ##################################################
        # links
        ##################################################

        feature['links'] = deepcopy(links)
        feature['links'][0]['href'] += i['collection'] + '/items/' + i['id']
        feature['links'][1]['href'] += i['collection']
        feature['links'][2]['href'] += i['collection']

        features.append(feature)

        # print('\nfeature: ')
        # pp.pprint(feature)
        # print('\n')

    gjson['features'] = features

    # logging.debug(f'make_geojson - gjson: {gjson}')

    return gjson


def make_json_item_collection(item_collection, params, matched, meta=None):
    # logging.debug(f'make_json_item_collection - item_collection: {item_collection}')

    # add 'context' extension to STAC
    # Specification: https://github.com/radiantearth/stac-spec/blob/v0.9.0/api-spec/extensions/context/README.md#context-extension-specification
    item_collection['stac_extensions'].append('context')

    item_collection['context'] = {
        'page': params['page'],
        'limit': params['limit'],
        'matched': matched,
        'returned': len(item_collection['features']),
        'meta': None if not meta else meta
    }

    return item_collection


def do_query(sql, **kwargs):
    start_time = time()

    connection = f'mysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}'
    engine = sqlalchemy.create_engine(connection)

    sql = text(sql)
    engine.execute('SET @@group_concat_max_len = 1000000;')

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
