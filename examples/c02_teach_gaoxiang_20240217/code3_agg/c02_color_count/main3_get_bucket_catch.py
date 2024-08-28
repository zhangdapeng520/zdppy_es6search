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
        "color_count": {
            "terms": {
                "field": "color.keyword"
            }
        }
    }
}
r = es.search(index, index, query)
print(r)
print(get_agg_buckets(r))
print(get_agg_buckets(r, "color_count"))
