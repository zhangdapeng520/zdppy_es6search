from ._source import get_search_source


def __bool(es, index, queries, doc_type=None, is_source=True, key="must"):
    """
    多条件且查询
    """
    if doc_type is None:
        doc_type = index

    # 校验
    if not isinstance(queries, list):
        return

    # 处理查询语句
    query = {
        "query": {
            "bool": {
                key: queries
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


def bool_must(es, index, queries, doc_type=None, is_source=True):
    """
    多条件且查询
    """
    return __bool(es, index, queries, doc_type, is_source, key="must")


def bool_should(es, index, queries, doc_type=None, is_source=True):
    """
    多条件或查询
    """
    return __bool(es, index, queries, doc_type, is_source, key="should")


def bool_must_not(es, index, queries, doc_type=None, is_source=True):
    """
    多条件非查询
    """
    return __bool(es, index, queries, doc_type, is_source, key="must_not")
