# 18 term等值查询

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
    {"id": 1, "name": "张三1", "ename": "zs1", "age": 23, "intro": "喜欢爬山，喜欢运动，喜欢一个人去旅行"},
    {"index": {"_index": index, "_type": index, "_id": "2"}},
    {"id": 2, "name": "张三2", "ename": "zs2", "age": 23, "intro": "宅家程序员，喜欢每天看看书，写写代码"},
    {"index": {"_index": index, "_type": index, "_id": "3"}},
    {"id": 3, "name": "张三3", "ename": "zs3", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "4"}},
    {"id": 4, "name": "zhangsan4", "ename": "zs4", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "5"}},
    {"id": 5, "name": "zhangsan love555", "ename": "zs4", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "6"}},
    {"id": 6, "name": "lisi", "ename": "李四", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
    {"index": {"_index": index, "_type": index, "_id": "7"}},
    {"id": 7, "name": "李四", "ename": "lisi", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
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
        "bool": {
            "must": [
                {"term": {"age": 23}},
                {"regexp": {"ename": "zs[1-2]"}}
            ]
        }
    }
}
r = es.search(index, index, query)
print(get_search_source(r))

# 删除索引
es.indices.delete(index)
