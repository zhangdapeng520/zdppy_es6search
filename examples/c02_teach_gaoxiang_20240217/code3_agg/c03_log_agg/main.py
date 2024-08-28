from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
# { "body" : "warning: page could not be rendered" }
index = "shop_log"
mappings = {
    "mappings": {
        index: {
            "properties": {
                "body": {"type": "text"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index}},
    {"body": "warning: page could not be rendered"},
    {"index": {"_index": index, "_type": index}},
    {"body": "authentication error"},
    {"index": {"_index": index, "_type": index}},
    {"body": "warning: connection timed out"},
    {"index": {"_index": index, "_type": index}},
    {"body": "info: hello pdai"},
]
es.bulk(data)
es.indices.refresh(index)


def get_search_source(r):
    data = {}

    # 第一层
    r = r.get("hits")
    if r is None:
        return data

    # 第二层
    r = r.get("hits")
    if r is None:
        return data

    # 提取source
    data = [v.get("_source") for v in r]
    return data


# 查询
query = {
    "query": {
        "match_all": {}
    }
}
r = es.search(index, index, query)
print(get_search_source(r))

# 删除索引
es.indices.delete(index)
