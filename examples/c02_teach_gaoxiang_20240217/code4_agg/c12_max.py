import json

from log import logger

from es6.elasticsearch import Elasticsearch

import es6search.agg

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
index = "shop_order"

r = es6search.agg.max(
    es,
    index,
    "color.keyword",
    "price"
)
print(r)
