from .query import get_query
from .buckets import get_buckets


def get_agg_histogram_query(field, interval, agg_field, agg_type="avg"):
    """
    获取聚合查询参数
    :param field: 要分组的字段，必须是date类型的字段
    :param interval: 间隔值，是一个整数
    :param agg_field: 聚合字段
    :param agg_type: 聚合类型：sum，min，max，avg，extended_stats
    """
    # 聚合查询语句
    agg_name = "histogram_agg"
    aggs = {
        agg_name: {
            "histogram": {
                "field": field,
                "interval": interval,  # 间隔
            },
            "aggs": {
                f"{field}_{agg_type}": {
                    agg_type: {
                        "field": agg_field,
                    }
                }
            }
        }
    }

    # 返回
    return agg_name, aggs


def histogram(
        es,
        index,
        field,
        interval,
        agg_field,
        agg_type="avg",
        query=None,
        doc_type=None,
):
    """
    将数据按照field列的interval间隔进行分组，然后再agg_field列根据agg_type进行聚合
    :param field: 要分组的字段，必须是date类型的字段
    :param interval: 间隔值，是一个整数
    :param agg_field: 聚合字段
    :param agg_type: 聚合类型：sum，min，max，avg，extended_stats
    :示例：r = es6search.agg.histogram(es,index,"price",20000,"price","sum")
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    agg_name, aggs = get_agg_histogram_query(field, interval,agg_field,agg_type)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)
