import es6search

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
# { "price" : 10000, "color" : "red", "make" : "honda", "sold" : "2014-10-28" }
index = "shop_order"

# 查询
agg_name, aggs = es6search.agg.get_agg_count_query("color", True)
query = es6search.agg.get_query(aggs)
r = es.search(index, index, query)
print(agg_name)
print(es6search.agg.get_buckets(r, agg_name))
