from .agg_query import get_agg_count_query
from .query import get_query
from .buckets import get_buckets


def get_agg_ip_query(field, ranges):
    """
    获取聚合查询参数
    :param field: 要分组的字段，必须是ip类型的字段
    :param ranges: 要分组的列表，比如
    [
        {"to": "192.168.127.14"},
        {"from": "192.168.127.14"},
    ]
    """
    # 聚合查询语句
    agg_name = "ip_agg"
    aggs = {
        agg_name: {
            "ip_range": {
                "field": field,
                "ranges": ranges
            }
        }
    }

    # 返回
    return agg_name, aggs


def ip(
        es,
        index,
        field,
        ranges,
        query=None,
        doc_type=None,
):
    """
    根据某列分组求另一列的均值
    :param field: 要分组的字段，必须是ip类型的字段
    :param ranges: 要分组的列表，比如
    [
        {"to": "192.168.127.14"},
        {"from": "192.168.127.14"},
    ]
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    agg_name, aggs = get_agg_ip_query(field, ranges)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)
