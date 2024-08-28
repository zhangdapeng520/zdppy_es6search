import es6search
from log import logger

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)


@logger.catch
def get_agg_buckets(result, agg_name, has_buckets=False):
    """提取聚合结果"""
    data = result.get("aggregations").get(agg_name)
    if has_buckets:
        return data.get("buckets")
    return data


# 创建索引
# { "price" : 10000, "color" : "red", "make" : "honda", "sold" : "2014-10-28" }
index = "shop_order"

# 查询
agg_name = "color_count"
aggs = {
    agg_name: {
        "terms": {
            "field": "color.keyword"
        }
    }
}
query = es6search.agg.get_query(aggs)

r = es.search(index, index, query)
print(get_agg_buckets(r, agg_name, True))
