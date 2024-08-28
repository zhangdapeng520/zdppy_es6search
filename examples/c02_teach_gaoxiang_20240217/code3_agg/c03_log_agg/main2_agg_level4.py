import json
from es6.elasticsearch import Elasticsearch
from log import logger

import es6search

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
# { "body" : "warning: page could not be rendered" }
index = "shop_log"
mappings = {
    "mappings": {
        index: {
            "properties": {
                "body": {"type": "text"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index}},
    {"body": "warning: page could not be rendered"},
    {"index": {"_index": index, "_type": index}},
    {"body": "authentication error"},
    {"index": {"_index": index, "_type": index}},
    {"body": "warning: connection timed out"},
    {"index": {"_index": index, "_type": index}},
    {"body": "info: hello pdai"},
    {"index": {"_index": index, "_type": index}},
    {"body": "info: warning: hello pdai"},
]
es.bulk(data)
es.indices.refresh(index)

# 查询
keywords = ["info", "warning"]
r = es6search.agg.match(
    es,
    index,
    keywords,
)
print(r)

# 删除索引
es.indices.delete(index)
