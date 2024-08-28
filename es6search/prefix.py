from ._source import get_search_source


def prefix(es, index, key, value, doc_type=None, is_text=False, is_source=True):
    """
    分页查询
    """
    if doc_type is None:
        doc_type = index

    # 对key做处理
    if is_text:
        key = f"{key}.keyword"

    # 处理查询语句
    query = {
        "query": {
            "prefix": {
                key: value,
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
