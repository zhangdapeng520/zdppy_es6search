# 04 基本分词查询

from es6.elasticsearch import Elasticsearch
import es6search

es = Elasticsearch(
    hosts=["http://192.168.234.129:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)
index = "user"
r = es6search.match(es, index, "intro", "爬山")
print(r)
