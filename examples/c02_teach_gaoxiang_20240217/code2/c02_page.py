import es6search
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
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ename": {"type": "keyword"},
                "age": {"type": "integer"},
                "intro": {"type": "text"},
                "address": {
                    "type": "nested",
                    "properties": {
                        "province": {"type": "keyword"},
                        "city": {"type": "keyword"},
                    }
                }
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index, "_id": "1"}},
    {"id": 1, "name": "张三1", "ename": "zs1", "age": 23, "intro": "喜欢爬山，喜欢运动，喜欢一个人去旅行"},
    {"index": {"_index": index, "_type": index, "_id": "2"}},
    {"id": 2, "name": "张三2", "ename": "zs2", "age": 23, "intro": "宅家程序员，喜欢每天看看书，写写代码"},
    {"index": {"_index": index, "_type": index, "_id": "3"}},
    {"id": 3, "name": "张三3", "ename": "zs3", "age": 23,
     "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "4"}},
    {"id": 4, "name": "zhangsan4", "ename": "zs4", "age": 23,
     "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "5"}},
    {"id": 5, "name": "zhangsan love555", "ename": "zs4", "age": 23,
     "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "6"}},
    {"id": 6, "name": "lisi", "ename": "李四", "age": 23,
     "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "7"}},
    {"id": 7, "name": "李四", "ename": "lisi", "age": 23,
     "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "8"}},
    {"id": 8, "name": "王五", "ename": "wangwu", "age": 25, "intro": "呵呵", "gender": "男"},
    {"index": {"_index": index, "_type": index, "_id": "9"}},
    {"id": 9, "name": "赵六", "ename": "zhaoliu", "age": 36, "intro": "老6",
     "address": {"province": "四川", "city": "成都"}},
    {"index": {"_index": index, "_type": index, "_id": "10"}},
    {"id": 10, "name": "赵六10", "ename": "zhaoliu10", "age": 36, "intro": "老6 10",
     "address": {"province": "北京", "city": "北京"}},
]
es.bulk(data)
es.indices.refresh(index)

# 搜索对象
ess = es6search.new(es)

# 核心代码 mauth amidauth amauth
print(ess.page(index, 0, 2))

# 删除索引
es.indices.delete(index)
