import es6search

from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
index = "shop_order"

# 查询
query = {
    "match_all": {}
}
r = es6search.agg.count(
    es,
    index,
    "retCode",
    query,
)
print(r)
