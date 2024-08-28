from ._source import get_search_source


def term(es, index, key, value, doc_type=None, is_text=False, is_source=True):
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
            "term": {
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


def terms(es, index, key, value, doc_type=None, is_text=False, is_source=True):
    """
    等值查询
    :param value 应该是list类型，形成类似SQL的in查询，比如：value=[1,2,3]
    """
    if doc_type is None:
        doc_type = index

    # 对value做校验
    if not isinstance(value, list) or len(value) == 0:
        return

    # 对key做处理
    if is_text:
        key = f"{key}.keyword"

    # 处理查询语句
    query = {
        "query": {
            "terms": {
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
