from .query import get_query
from .buckets import get_buckets


def get_agg_query(field1, field2, agg_field, agg_type="sum"):
    """
    获取聚合查询参数
    :param field1: 分组字段1
    :param field2: 分组字段2
    :param agg_field: 聚合字段
    :param agg_type: 聚合类型 avg,sum,max,min,extended_stat
    """
    # 聚合查询语句
    aggs = {
        field1: {
            "terms": {
                "field": field1
            },
            "aggs": {
                field2: {
                    "terms": {
                        "field": field2
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
        }
    }

    # 返回
    return aggs


def handle_result(field1, field2, agg_field, agg_type, result_type=None, r=None):
    """
    :param result_type: 结果类型，如果为None，则默认是字典类型。如果是list则是列表类型。
    """
    # 结果
    data = {}
    if result_type is not None:
        data = []

    # 遍历
    for v in r:
        # 字段1
        key1 = v.get("key")
        # 字段2
        field2_buckets = v.get(field2).get("buckets")
        if len(field2_buckets) == 0:
            continue
        key2 = field2_buckets[0].get("key")
        # 聚合值
        for vv in field2_buckets:
            value = vv.get(f"{agg_field}_{agg_type}").get("value")
            if result_type is not None:
                data.append([key1, key2, value])
            else:
                data[f"{key1}_{key2}"] = {
                    field1: key1,
                    field2: key2,
                    agg_type: value
                }
    return data


def group2(
        es,
        index,
        field1,
        field2,
        agg_field,
        agg_type="sum",
        query=None,
        doc_type=None,
        result_type=None,
):
    """
    根据某列分组求另一列的均值
    :param field1: 分组字段1
    :param field2: 分组字段2
    :param agg_field: 聚合字段
    :param agg_type: 聚合类型 avg,sum,max,min,extended_stat
    """
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    aggs = get_agg_query(field1, field2, agg_field, agg_type)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 得到聚合结果
    r = get_buckets(r, field1)

    # 处理聚合结果
    data = handle_result(field1, field2, agg_field, agg_type, result_type, r)

    # 返回
    return data
