def get_query(aggs=None, query=None):
    """获取查询参数"""
    # 基本结构
    result = {
        "query": {
            "match_all": {}
        },
        "size": 0,
        "aggs": {}
    }

    # query校验
    if isinstance(query, dict):
        # query填充
        result["query"] = query

    # aggs校验
    if isinstance(aggs, dict):
        result["aggs"] = aggs

    # 返回
    return result
