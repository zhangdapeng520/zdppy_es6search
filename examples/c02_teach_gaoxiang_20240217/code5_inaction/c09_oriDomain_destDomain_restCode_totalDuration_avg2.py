from es6.elasticsearch import Elasticsearch

import es6search

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
index = "shop_order"

# 查询
r = es6search.agg.group3(
    es,
    index,
    "oriDomain.keyword",
    "destDomain.keyword",
    "retCode",
    "totalDuration",
    "avg",
)
print(r)

# 查询
r = es6search.agg.group3(
    es,
    index,
    "oriDomain.keyword",
    "destDomain.keyword",
    "retCode",
    "totalDuration",
    "avg",
    result_type="list",
)
print(r)
