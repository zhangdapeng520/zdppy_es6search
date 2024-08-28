import json
from es6.elasticsearch import Elasticsearch
from log import logger

import es6search

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
index = "shop_order"
field = "price"
ranges = [
    {"to": 20000},
    {"from": 20000, "to": 40000},  # 包含from，即就是左闭右开
    {"from": 40000},
]
r = es6search.agg.num(
    es,
    index,
    field,
    ranges
)
print(r)
