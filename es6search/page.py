from ._source import get_search_source


def page(es, index, offset=0, size=20, query=None, doc_type=None, is_source=True):
    """
    分页查询
    """
    if doc_type is None:
        doc_type = index

    # 处理查询语句
    if query is None:
        query = {
            "query": {
                "match_all": {}
            }
        }

    # 执行搜索
    r = es.search(index, doc_type, query, from_=offset, size=size)

    # 提取结果
    if is_source:
        r = get_search_source(r)

    # 返回
    return r
