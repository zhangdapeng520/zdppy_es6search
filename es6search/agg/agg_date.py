from .query import get_query
from .buckets import get_buckets


def get_agg_date_query(field, ranges, format="yyyy-MM-dd"):
    """
    获取聚合查询参数
    :param field: 要分组的字段，必须是date类型的字段
    :param format: 日期格式，默认 2014-01-01 格式
    :param ranges: 要分组的列表，比如
    [
        {"to": "2014-01-01"},
        {"from": "2014-01-01", "to": "2014-12-31"},
        {"from": "2014-12-31"},
    ]
    """
    # 聚合查询语句
    agg_name = "date_agg"
    aggs = {
        agg_name: {
            "date_range": {
                "field": field,
                "format": format,
                "ranges": ranges
            }
        }
    }

    # 返回
    return agg_name, aggs


def date(
        es,
        index,
        field,
        ranges,
        format="yyyy-MM-dd",
        query=None,
        doc_type=None,
):
    """
    根据某列分组求另一列的均值
    :param format: 日期格式，默认 2014-01-01 格式
    :param field: 要分组的字段，必须是date类型的字段
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
    agg_name, aggs = get_agg_date_query(field, ranges, format)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)
