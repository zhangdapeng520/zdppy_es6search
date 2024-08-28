# 01 基本实现分页查询

import es6search
from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.129:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)
index = "user"

# 查询
r = es6search.page(es, index, 0, 2)
print(r)
