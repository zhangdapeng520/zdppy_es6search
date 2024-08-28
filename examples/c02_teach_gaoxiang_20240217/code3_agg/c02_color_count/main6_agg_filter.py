import json

from log import logger

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)


@logger.catch
def get_agg_buckets(result, agg_name):
    """提取聚合结果"""
    return result.get("aggregations").get(agg_name).get("buckets")


def write_json(result):
    with open("test.json", "w") as f:
        json.dump(result, f)


# 创建索引
# { "price" : 10000, "color" : "red", "make" : "honda", "sold" : "2014-10-28" }
index = "shop_order"

# 查询
query = {
    "query": {
        "match_all": {}
    },
    "size": 0,
    "aggs": {
        "make_by": {
            # 查询过滤
            "filter": {
                "term": {
                    "make": "honda",  # honda 是 make 的一种值
                }
            },
            "aggs": {
                "avg_price": {
                    "avg": {  # avg 是 es 的聚合函数
                        "field": "price",
                    }
                }
            }
        }
    }
}
r = es.search(index, index, query)
write_json(r)
