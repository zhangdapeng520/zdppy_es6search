# 09 新增文档

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

# 删除索引
es.indices.delete(index)
