from .query import get_query
from .buckets import get_buckets


def get_agg_num_query(field, ranges):
    """
    获取聚合查询参数
    :param field: 要分组的字段，必须是num类型的字段
    :param ranges: 要分组的列表，比如
    [
      {"to": 20000},
      {"from": 20000, "to": 40000},  # 包含from，即就是左闭右开
      {"from": 40000},
    ]
    """
    # 聚合查询语句
    agg_name = "num_agg"
    aggs = {
        agg_name: {
            "range": {
                "field": field,
                "ranges": ranges,
            },
        }
    }

    # 返回
    return agg_name, aggs


def num(
        es,
        index,
        field,
        ranges,
        query=None,
        doc_type=None,
):
    """
    根据某列分组求另一列的均值
    :param field: 要分组的字段，必须是num类型的字段
    :param ranges: 要分组的列表，比如
    [
        {"to": 20000},
        {"from": 20000, "to": 40000},  # 包含from，即就是左闭右开
        {"from": 40000},
    ]
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    agg_name, aggs = get_agg_num_query(field, ranges)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)
