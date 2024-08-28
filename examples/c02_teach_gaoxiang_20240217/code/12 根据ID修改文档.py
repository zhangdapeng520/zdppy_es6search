# 12 根据ID修改文档

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.129:9200"],
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
zs = {
    "id": 1,
    "name": "张三",
    "age": 23,
}
es.index(index, index, zs, id=zs.get("id"))
es.indices.refresh(index)

# 修改
body = {
    "doc": {
        "id": 1,
        "name": "张三333",
        "age": 23,
    }
}
es.update(index, index, zs.get("id"), body)
es.indices.refresh(index)

# 查询
query = {
    "query": {
        "match_all": {}
    }
}
r = es.search(index, index, query)
print(r.get("hits").get("hits")[0].get("_source"))

# 删除索引
es.indices.delete(index)
