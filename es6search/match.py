from ._source import get_search_source


def __match(es, index, key, value, doc_type=None, is_source=True, query_key="match"):
    """
    分词查询
    """
    if doc_type is None:
        doc_type = index

    # 构造查询条件
    query = {
        "query": {
            query_key: {
                key: value
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


def match(es, index, key, value, doc_type=None, is_source=True):
    """
    分词查询
    """
    return __match(es, index, key, value, doc_type, is_source, "match")


def match_phrase(es, index, key, value, doc_type=None, is_source=True):
    """
    短语查询
    """
    return __match(es, index, key, value, doc_type, is_source, "match_phrase")


def multi_match(es, index, key, value, doc_type=None, is_source=True):
    """
    多字段分词查询
    :param key 你要查询的关键字的值
    :param value 是列表类型，指定你要查询的列名
    """
    if doc_type is None:
        doc_type = index

    # 关键字不能为空
    if key is None:
        return

    # value必须是list类型且有值
    if not isinstance(value, list) or len(value) == 0:
        return

        # 封装查询参数
    query = {
        "query": {
            "multi_match": {
                "query": key,
                "fields": value
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
