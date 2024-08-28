from .query import get_query
from .buckets import get_buckets


def get_agg_count_query(field):
    """
    获取聚合查询参数
    :param field: 要聚合的字段
    """
    # 聚合查询语句
    aggs = {
        field: {
            "terms": {
                "field": field
            }
        }
    }

    # 返回
    return aggs


def count(
        es,
        index,
        field,
        query=None,
        doc_type=None,
):
    """根据某列分组聚合求个数"""
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    aggs = get_agg_count_query(field)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, field)
