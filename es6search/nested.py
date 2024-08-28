from ._source import get_search_source


def nested(es, index, field, query, doc_type=None, is_source=True):
    """
    嵌套查询
    """
    if doc_type is None:
        doc_type = index

    # 处理查询语句
    query = {
        "query": {
            "nested": {
                "path": field,
                "query": query
            }
        }
    }

    # 执行搜索
    r = es.search(index, doc_type, query)

    # 提取结果
    if is_source:
        r = get_search_source(r)

    # 返回
    return r
