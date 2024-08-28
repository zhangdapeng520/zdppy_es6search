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
                "intro": {"type": "text"},
            }
        }
    }
}
try:
    es.indices.delete(index)
except:
    pass
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index, "_id": "1"}},
    {"id": 1, "name": "张三1", "age": 23, "intro": "喜欢爬山，喜欢运动，喜欢一个人去旅行"},
    {"index": {"_index": index, "_type": index, "_id": "2"}},
    {"id": 2, "name": "张三2", "age": 23, "intro": "宅家程序员，喜欢每天看看书，写写代码"},
    {"index": {"_index": index, "_type": index, "_id": "3"}},
    {"id": 3, "name": "张三3", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
]
es.bulk(data)
es.indices.refresh(index)
