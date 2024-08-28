from .query import get_query
from .buckets import get_buckets


def get_agg_num_query(field, ranges, agg_field, agg_type):
    """
    获取聚合查询参数
    :param field: 要分组的字段，必须是num类型的字段
    :param agg_field: 要聚合的字段
    :param agg_type: 要聚合的类型：sum,min,max,avg,count
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
                "ranges": ranges
            },
            "aggs": {
                f"{agg_field}_{agg_type}": {
                    agg_type: {
                        "field": agg_field
                    }
                }
            }
        }
    }

    # 返回
    return agg_name, aggs


def numagg(
        es,
        index,
        field,
        ranges,
        agg_field,
        agg_type="avg",
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
    agg_name, aggs = get_agg_num_query(field, ranges, agg_field, agg_type)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)
