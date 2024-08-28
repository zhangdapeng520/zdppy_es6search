def get_agg_count_query(field, is_text_keyword=False):
    """
    获取聚合查询参数
    :param field: 要聚合的字段
    :param is_text_keyword: 该字段是否为text类型，但是支持keyword查询
    """
    # 聚合名称
    agg_name = f"{field}_count"

    # 判断field
    if is_text_keyword:
        field = f"{field}.keyword"

    # 聚合查询语句
    aggs = {
        agg_name: {
            "terms": {
                "field": field
            }
        }
    }

    # 返回
    return agg_name, aggs
