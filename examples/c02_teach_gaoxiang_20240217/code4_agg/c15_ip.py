import json
from es6.elasticsearch import Elasticsearch
from log import logger

import es6search

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
# 比较逻辑：按点拆分，从左到右依次比较。只要有一段发生大小不一样，就按该轮大小计算。
# 比如： 192.168.255.255 和 193.0.0.0 , 因为第一段的时候，193比192大，所以认为 193.0.0.0 比 192.168.255.255 大
# 和字符串的比较逻辑一致
# abc 和 aac，会分别将每个字符串转换为ASCII编码，然后从左到右依次比较大小，遇到能不同则按该轮大小计算
# 97 98 99  和 97 97 99
data = [
    {"index": {"_index": index, "_type": index}},
    {"ip": "192.168.126.12"},
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
    {"index": {"_index": index, "_type": index}},
    {"ip": "193.1.1.1"},
]
es.bulk(data)
es.indices.refresh(index)

field = "ip"
ranges = [
    {"to": "192.168.127.14"},
    {"from": "192.168.127.14"},
]
r = es6search.agg.ip(
    es,
    index,
    field,
    ranges
)
print(r)

# 删除索引
es.indices.delete(index)
