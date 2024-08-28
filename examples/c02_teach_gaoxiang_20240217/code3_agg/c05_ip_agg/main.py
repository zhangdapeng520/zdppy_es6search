import json
from es6.elasticsearch import Elasticsearch
from log import logger

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
# { "price" : 10000, "color" : "red", "make" : "honda", "sold" : "2014-10-28" }
index = "shop_ip"
mappings = {
    "mappings": {
        index: {
            "properties": {
                "ip": {"type": "ip"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index}},
    {"ip": "192.168.127.12"},
    {"index": {"_index": index, "_type": index}},
    {"ip": "192.168.127.13"},
    {"index": {"_index": index, "_type": index}},
    {"ip": "192.168.127.14"},
    {"index": {"_index": index, "_type": index}},
    {"ip": "192.168.128.13"},
    {"index": {"_index": index, "_type": index}},
    {"ip": "192.168.128.14"},
]
es.bulk(data)
es.indices.refresh(index)


@logger.catch
def get_agg_buckets(result, agg_name, has_buckets=False):
    """提取聚合结果"""
    data = result.get("aggregations").get(agg_name)
    if has_buckets:
        return data.get("buckets")
    return data


def write_json(result):
    with open("test.json", "w") as f:
        json.dump(result, f)


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
