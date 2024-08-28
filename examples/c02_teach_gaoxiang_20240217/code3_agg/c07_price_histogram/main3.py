import json
from es6.elasticsearch import Elasticsearch
from log import logger

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
# { "price" : 10000, "color" : "red", "make" : "honda", "sold" : "2014-10-28" }
index = "shop_order4"
mappings = {
    "mappings": {
        index: {
            "properties": {
                "price": {"type": "integer"},
                "color": {"type": "text"},
                "make": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "sold": {"type": "date"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index}},
    {"price": 10000, "color": "red", "make": "honda", "sold": "2013-10-28"},
    {"index": {"_index": index, "_type": index}},
    {"price": 20000, "color": "red", "make": "honda", "sold": "2014-11-05"},
    {"index": {"_index": index, "_type": index}},
    {"price": 30000, "color": "green", "make": "ford", "sold": "2014-05-18"},
    {"index": {"_index": index, "_type": index}},
    {"price": 15000, "color": "blue", "make": "toyota", "sold": "2014-07-02"},
    {"index": {"_index": index, "_type": index}},
    {"price": 12000, "color": "green", "make": "toyota", "sold": "2014-08-19"},
    {"index": {"_index": index, "_type": index}},
    {"price": 20000, "color": "red", "make": "honda", "sold": "2014-11-05"},
    {"index": {"_index": index, "_type": index}},
    {"price": 80000, "color": "red", "make": "bmw", "sold": "2014-01-01"},
    {"index": {"_index": index, "_type": index}},
    {"price": 25000, "color": "blue", "make": "ford", "sold": "2015-02-12"},
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


# 查询
agg_name = "sold_range"
query = {
    "size": 0,
    "aggs": {
        agg_name: {
            "terms": {
                "field": "make.keyword",
                "size": 10,  # 排序后的前10条？
            },
            "aggs": {
                "agg_detail": {
                    # 详细统计信息
                    "extended_stats": {
                        "field": "price",
                    }
                }
            }
        }
    }
}
r = es.search(index, index, query)
print(get_agg_buckets(r, agg_name, True))

# 删除索引
es.indices.delete(index)
