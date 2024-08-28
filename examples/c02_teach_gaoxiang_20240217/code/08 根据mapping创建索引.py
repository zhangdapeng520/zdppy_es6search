# 08 根据mapping创建索引

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

# 删除索引
es.indices.delete(index)
