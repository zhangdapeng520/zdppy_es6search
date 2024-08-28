from es6.elasticsearch.helpers import scan


def scan_list(
        es,
        index,
        query,
        scroll="1m",
        request_timeout=60,
        size=10000,
):
    """
    扫描并转换为list集合
    """
    _searched = scan(
        client=es,
        query=query,
        scroll=scroll,
        index=index,
        request_timeout=request_timeout,
        size=size,
    )
    return [doc.get("_source", {}) for doc in _searched]
