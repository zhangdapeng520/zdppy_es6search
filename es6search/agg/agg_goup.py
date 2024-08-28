from .agg_query import get_agg_count_query
from .query import get_query
from .buckets import get_buckets


def get_agg_group_query(group_field, agg_field, agg_type):
    """
    获取聚合查询参数
    :param group_field: 要分组的字段
    :param agg_field: 要聚合值的字段
    :param agg_type: 聚合类型
    """
    # 聚合查询语句
    aggs = {
        group_field: {
            "terms": {
                "field": group_field
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
    return group_field, aggs


def group(
        es,
        index,
        group_field,
        agg_field,
        agg_type="avg",
        query=None,
        doc_type=None,
):
    """
    根据某列分组求另一列的均值
    :param group_field: 要分组的字段
    :param agg_field: 要聚合值的字段
    :param agg_type: 聚合类型：avg,sum,min,max,extended_stat
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    agg_name, aggs = get_agg_group_query(group_field, agg_field, agg_type)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return get_buckets(r, agg_name)


def groups(
        es,
        index,
        field_list,
        doc_type=None,
        query=None,
):
    """
    根据某列分组聚合求个数
    :param field_list: 字段列表，比如：[["color.keyword","price","sum"],["make.keyword","price","avg"]]
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # 校验field_list
    if not isinstance(field_list, list):
        return
    if len(field_list) == 0:
        return
    if not isinstance(field_list[0], list):
        return
    if len(field_list[0]) != 3:
        return

    # agg聚合查询参数
    aggs_query = {}
    agg_name_list = []
    for field in field_list:
        # 必须形如：["color.keyword","price","avg"]
        if len(field) != 3:
            return
        agg_name, aggs = get_agg_group_query(field[0], field[1], field[2])
        agg_name_list.append(agg_name)
        aggs_query.update(aggs)

    # es查询参数
    agg_query = get_query(aggs_query, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 封装返回值
    result = {}
    for agg_name in agg_name_list:
        result[agg_name] = get_buckets(r, agg_name)

    # 返回
    return result
