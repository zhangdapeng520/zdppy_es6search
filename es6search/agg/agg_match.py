from .query import get_query
from .buckets import get_buckets


def get_agg_match_query(*keywords):
    """
    获取聚合查询参数
    :param keywords: 要分组的关键字列表
    """
    # 封装filters语句
    filters = {}
    for keyword in keywords:
        filters[keyword] = {"match": {"body": keyword}}

    # 封装aggs查询语句
    agg_name = "level_group"
    aggs = {
        # 聚合名称
        agg_name: {
            "filters": {
                # 没有被筛选到的分到此组
                "other_bucket_key": "other",
                "filters": filters,

            },
        }
    }

    # 返回
    return agg_name, aggs


def match(
        es,
        index,
        keywords,
        query=None,
        doc_type=None,
):
    """
    根据分词关键字求count
    :param keywords: 要分组的分词关键字列表
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    agg_name, aggs = get_agg_match_query(*keywords)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)
