from .query import get_query
from .buckets import get_buckets


def get_agg_count_query(field1, field2, field3):
    """
    获取聚合查询参数
    :param field1: 要聚合的字段1
    :param field2: 要聚合的字段2
    :param field3: 要聚合的字段3
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
                        field3: {
                            "terms": {
                                "field": field3
                            },
                        }
                    }
                },
            }
        }
    }

    # 返回
    return aggs


def handle_result(r, field1, field2, field3, result_type="dict"):
    """对结果做处理"""
    # 提取聚合结果
    data = r.get("aggregations")
    if not isinstance(data, dict):
        return

    # 提取第一层聚合结果
    field1_data = data.get(field1)
    if not isinstance(field1_data, dict):
        return

    # 提取第一层聚合数据
    field1_data_buckets = field1_data.get("buckets")
    if not isinstance(field1_data_buckets, list):
        return

    # 遍历
    data = []
    for v in field1_data_buckets:
        key1 = v.get("key")
        field2_data = v.get(field2)
        if not isinstance(field2_data, dict):
            continue
        field2_data_buckets = field2_data.get("buckets")
        if not isinstance(field2_data_buckets, list):
            continue

        # 第二层数据
        for vv in field2_data_buckets:
            key2 = vv.get("key")
            field3_data = vv.get(field3)
            if not isinstance(field3_data, dict):
                continue
            field3_data_buckets = field3_data.get("buckets")
            if not isinstance(field3_data_buckets, list):
                continue
            # 封装数据
            for vvv in field3_data_buckets:
                key3 = vvv.get("key")
                count = vvv.get("doc_count")
                if result_type == "dict":
                    data.append({
                        field1: key1,
                        field2: key2,
                        field3: key3,
                        "doc_count": count,
                    })
                else:
                    data.append([key1, key2, key3, count])

    return data


def count3(
        es,
        index,
        field1,
        field2,
        field3,
        result_type="dict",
        query=None,
        doc_type=None,
):
    """根据某列分组聚合求个数"""
    # 处理doc_type
    if doc_type is None:
        doc_type = index

    # agg聚合查询参数
    aggs = get_agg_count_query(field1, field2, field3)

    # es查询参数
    agg_query = get_query(aggs, query)

    # 执行查询
    r = es.search(index, doc_type, agg_query)

    # 返回
    return handle_result(r, field1, field2, field3, result_type)
