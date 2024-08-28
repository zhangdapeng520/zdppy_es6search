from .agg_query import get_agg_count_query
from .query import get_query
from .buckets import get_buckets


def filter(
        es,
        index,
        field=None,
        agg_type="avg",
        filter_query=None,
        query=None,
        doc_type=None,
):
    """
    根据某列分组求另一列的均值
    :param filter_query: 进行过滤的查询语句
    :param field: 要求均值的字段
    :param agg_type: 聚合类型，支持：avg,sum,min,max
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # 参数校验
    if filter_query is None:
        filter_query = {
            "match_all": {}
        }

    # agg聚合查询参数
    agg_name, aggs = get_agg_filter_query(filter_query, field, agg_type)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)


def get_agg_filter_query(filter_query, field, agg_type="avg"):
    """
    获取聚合查询参数
    :param filter_query: 用来过滤数据的查询语句
    :param field: 要求均值的字段
    :param agg_type: 聚合类型，支持：avg,sum,min,max,extended_stats
    """
    # 聚合查询语句
    agg_name = "filter_agg"
    aggs = {
        agg_name: {
            "filter": filter_query,
            "aggs": {
                f"{field}_{agg_type}": {
                    agg_type: {
                        "field": field
                    }
                }
            }
        }
    }

    # 返回
    return agg_name, aggs
