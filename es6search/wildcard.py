from ._source import get_search_source


def wildcard(es, index, key, value, doc_type=None, is_text=False, is_source=True):
    """
    通配符查看，可以使用?代替一个字符进行查询
    :param value 带通配符的值，必须是字符串类型，必须有通配符?或者*
    """
    if doc_type is None:
        doc_type = index

    # 对key做处理
    if is_text:
        key = f"{key}.keyword"

    # 校验value
    if not isinstance(value, str):
        return
    if "*" not in value and "?" not in value:
        return

    # 处理查询语句
    query = {
        "query": {
            "wildcard": {
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
