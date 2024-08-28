from ._source import get_search_source


def range(es, index, key, gt=None, lt=None, gte=None, lte=None, doc_type=None, is_source=True):
    """
    范围查询
    """
    if doc_type is None:
        doc_type = index

    # 封装查询字典
    range_dict = {}
    if gt is not None:
        range_dict["gt"] = gt
    if lt is not None:
        range_dict["lt"] = lt
    if gte is not None:
        range_dict["gte"] = gte
    if lte is not None:
        range_dict["lte"] = lte

    # 处理查询语句
    query = {
        "query": {
            "range": {
                key: range_dict,
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
