# 13 批量新增数据

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
index = "user"
mappings = {
    "mappings": {
        "user": {
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "text"},
                "age": {"type": "integer"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index, "_id": "1"}},
    {"id": 1, "name": "张三1", "age": 23, },
    {"index": {"_index": index, "_type": index, "_id": "2"}},
    {"id": 2, "name": "张三2", "age": 23, },
    {"index": {"_index": index, "_type": index, "_id": "3"}},
    {"id": 3, "name": "张三3", "age": 23, },
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
