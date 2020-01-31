#!/usr/bin/env python3

import os
from json import loads
from pprint import PrettyPrinter

from datetime import datetime
from copy import deepcopy
from collections import OrderedDict

import sqlalchemy
from sqlalchemy.sql import text

from inpe_stac.log import logging
from inpe_stac.decorator import log_function_header


pp = PrettyPrinter(indent=4)


def len_result(result):
    return len(result) if result is not None else len([])


@log_function_header
def get_collections(collection_id=None):
    logging.info('get_collections - collection_id: {}'.format(collection_id))

    kwargs = {}
    where = ''

    # if there is a 'collection_id' key to search, then add the WHERE clause and the key to kwargs
    if collection_id is not None:
        where = 'WHERE id = :collection_id'
        kwargs = { 'collection_id': collection_id }

    query = 'SELECT * FROM collection {};'.format(where)

    logging.info('get_collections - query: {}'.format(query))

    result = do_query(query, **kwargs)

    logging.info('get_collections - len(result): {}'.format(len_result(result)))
    # logging.debug('get_collections - result: {}'.format(result))

    return result


@log_function_header
def get_collection_items(collection_id=None, item_id=None, bbox=None, time=None,
                         intersects=None, page=1, limit=10, ids=None, collections=None):

    params = deepcopy(locals())
    params['page'] = (page - 1) * limit

    query = '\nSELECT * FROM item \nWHERE '
    query_count = '\nSELECT COUNT(id) as matched FROM item \nWHERE '

    where = []

    if ids is not None:
        where.append('FIND_IN_SET(id, :ids)')
    elif item_id is not None:
        where.append('id = :item_id')
    else:
        if collections is not None:
            where.append('FIND_IN_SET(collection, :collections)')
        elif collection_id is not None:
            where.append('collection = :collection_id')

        if bbox is not None:
            try:
                for x in bbox.split(','):
                    float(x)

                params['min_x'], params['min_y'], params['max_x'], params['max_y'] = bbox.split(',')

                # replace method removes extra espace caused by multi-line String
                where.append(
                    '''(
                    ((:min_x <= TR_Longitude and :min_y <= TR_Latitude)
                    or
                    (:min_x <= BR_Longitude and :min_y <= TL_Latitude))
                    and
                    ((:max_x >= BL_Longitude and :max_y >= BL_Latitude)
                    or
                    (:max_x >= TL_Longitude and :max_y >= BR_Latitude))
                    )'''.replace('                ', '')
                )
            except:
                raise (InvalidBoundingBoxError())

            if time is not None:
                if "/" in time:
                    params['time_start'], params['time_end'] = time.split("/")
                    where.append("date <= :time_end")
                else:
                    params['time_start'] = time

                where.append("date >= :time_start")

    # create where and limit clauses
    where = '\nAND '.join(where)
    limit = '\nLIMIT :page, :limit'

    # add just where clause to query, because I want to get the number of total results
    query_count += where
    # add where and limit clauses to query
    query += where + limit

    logging.info('get_collection_items - params: {}'.format(params))
    # logging.info('get_collection_items - query_count: {}'.format(query_count))
    logging.info('get_collection_items - query: {}'.format(query))

    # execute the queries
    result_count = do_query(query_count, **params)
    result = do_query(query, **params)

    matched = result_count[0]['matched']

    logging.info('get_collection_items - returned: {}'.format(len_result(result)))
    logging.debug('get_collection_items - matched: {}'.format(matched))
    # logging.debug('get_collection_items - result: {}'.format(result))

    return result, matched


def make_geojson(items, links):
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
        # print('\n\nid: ', i['id'])
        # print('item: ', end='')
        # pp.pprint(i)
        # print('\n\n')

        feature = OrderedDict()

        feature['type'] = 'Feature'
        feature['id'] = i['id']
        feature['collection'] = i['collection']

        geometry = dict()
        geometry['type'] = 'Polygon'
        geometry['coordinates'] = [
          [[i['TL_Longitude'], i['TL_Latitude']],
           [i['BL_Longitude'], i['BL_Latitude']],
           [i['BR_Longitude'], i['BR_Latitude']],
           [i['TR_Longitude'], i['TR_Latitude']],
           [i['TL_Longitude'], i['TL_Latitude']]]
        ]
        feature['geometry'] = geometry
        feature['bbox'] = bbox(feature['geometry']['coordinates'])

        feature['properties'] = {
            'datetime': datetime.fromisoformat(str(i['date'])).isoformat(),
            'path': i['path'],
            'row': i['row'],
            'satellite': i['satellite'],
            'sensor': i['sensor']
        }

        feature['assets'] = {}

        # convert string json to dict json
        i['assets'] = loads(i['assets'])

        for asset in i['assets']:
            feature['assets'][asset['band']] = {'href': os.getenv('TIF_ROOT') + asset['href']}

        feature['assets']['thumbnail'] = {'href': os.getenv('PNG_ROOT') + i['thumbnail']}

        feature['links'] = deepcopy(links)
        feature['links'][0]['href'] += i['collection'] + "/items/" + i['id']
        feature['links'][1]['href'] += i['collection']
        feature['links'][2]['href'] += i['collection']

        features.append(feature)

        # print('\nfeature: ')
        # pp.pprint(feature)
        # print('\n')

    if len(features) == 1:
        return features[0]

    gjson['features'] = features

    return gjson


def do_query(sql, **kwargs):
    connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
                                              os.environ.get('DB_PASS'),
                                              os.environ.get('DB_HOST'),
                                              os.environ.get('DB_NAME'))
    engine = sqlalchemy.create_engine(connection)

    sql = text(sql)
    engine.execute("SET @@group_concat_max_len = 1000000;")

    result = engine.execute(sql, kwargs)
    result = result.fetchall()

    engine.dispose()

    result = [dict(row) for row in result]

    if len(result) > 0:
        return result
    else:
        return None


def bbox(coord_list):
    box = []

    for i in (0, 1):
        res = sorted(coord_list[0], key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))

    return [box[0][0], box[1][0], box[0][1], box[1][1]]


class InvalidBoundingBoxError(Exception):
    pass
