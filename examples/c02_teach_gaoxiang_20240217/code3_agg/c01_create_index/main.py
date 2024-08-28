from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
# { "price" : 10000, "color" : "red", "make" : "honda", "sold" : "2014-10-28" }
index = "shop_order"
mappings = {
    "mappings": {
        "shop_order": {
            "properties": {
                "price": {"type": "integer"},
                "color": {"type": "text"},
                "make": {"type": "text"},
                "sold": {"type": "date"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = [
    {"index": {"_index": index, "_type": index}},
    {"price": 10000, "color": "red", "make": "honda", "sold": "2014-10-28"},
    {"index": {"_index": index, "_type": index}},
    {"price": 20000, "color": "red", "make": "honda", "sold": "2014-11-05"},
    {"index": {"_index": index, "_type": index}},
    {"price": 30000, "color": "green", "make": "ford", "sold": "2014-05-18"},
    {"index": {"_index": index, "_type": index}},
    {"price": 15000, "color": "blue", "make": "toyota", "sold": "2014-07-02"},
    {"index": {"_index": index, "_type": index}},
    {"price": 12000, "color": "green", "make": "toyota", "sold": "2014-08-19"},
    {"index": {"_index": index, "_type": index}},
    {"price": 20000, "color": "red", "make": "honda", "sold": "2014-11-05"},
    {"index": {"_index": index, "_type": index}},
    {"price": 80000, "color": "red", "make": "bmw", "sold": "2014-01-01"},
    {"index": {"_index": index, "_type": index}},
    {"price": 25000, "color": "blue", "make": "ford", "sold": "2014-02-12"},
]
es.bulk(data)
es.indices.refresh(index)


def get_search_source(r):
    data = {}

    # 第一层
    r = r.get("hits")
    if r is None:
        return data

    # 第二层
    r = r.get("hits")
    if r is None:
        return data

    # 提取source
    data = [v.get("_source") for v in r]
    return data


# 查询
query = {
    "query": {
        "match_all": {}
    }
}
r = es.search(index, index, query)
print(get_search_source(r))

# 删除索引
es.indices.delete(index)
