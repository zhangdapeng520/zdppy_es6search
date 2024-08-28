def get_buckets(result, agg_name):
    """提取聚合结果"""
    # 结果
    data = None

    # 校验
    if not isinstance(result, dict):
        return data

    # 提取聚合值
    data = result.get("aggregations")
    if not isinstance(data, dict):
        return data

    # 提取聚合值
    data = data.get(agg_name)
    if not isinstance(data, dict):
        return data

    # 提取桶
    buckets = data.get("buckets")
    if not (isinstance(buckets, list) or isinstance(buckets, dict)):
        return data

    # 返回
    return buckets
