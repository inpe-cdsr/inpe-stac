
from inpe_stac.environment import INPE_STAC_DELETED


def calc_offset(page, limit):
    # example: limit = 10
    # if page is equals 0, then it will be 10*0 = 0 (first page)
    # if page is equals 1, then it will be 10*1 = 10 (second page)
    # etc.

    # get the page in Array-like in order to
    page = page - 1

    if page < 0:
        page = 0

    return page * limit


def get_query_string(params):
    return '&'.join([
        f'{k}={v}' for k, v in params.items() if v is not None
    ])


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


def len_result(result):
    return len(result) if result is not None else len([])
