# 06 查询所有数据

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)
print(es)

# 新增文档，如果索引不存在，会自动创建
zs = {
    "id": "1",
    "name": "张三",
    "age": 23
}
r = es.index(index="user", doc_type="user", id="1", body=zs)
print(r)

# 查询
r = es.search(index="user")
print(r)
r = es.search(index="user", body={"query": {"match_all": {}}})
print(r)
print(r["hits"]["hits"][0]["_source"])

# 刷新索引
es.indices.refresh("user")
