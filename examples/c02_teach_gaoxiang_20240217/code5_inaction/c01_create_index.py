import random
from es6.elasticsearch import Elasticsearch

es = Elasticsearch(
    hosts=["http://192.168.234.130:9200"],
    http_auth=("elastic", "zhangdapeng520"),
)

# 创建索引
index = "shop_order"
mappings = {
    "mappings": {
        index: {
            "properties": {
                "callCount": {"type": "integer"},
                "oriDomain": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "childSystem": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "destDomain": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "oriMicroService": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "destMicroService": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "totalDuration": {"type": "integer"},
                "retCode": {"type": "integer"},
            }
        }
    }
}
es.indices.create(index, mappings)

# 新增
data = []
for i in range(10000):
    data.append({"index": {"_index": index, "_type": index}})
    item = {
        "callCount": random.randint(3, 10),
        "oriDomain": f"gmis-prod-{i % 10}",
        "childSystem": f"gmis-prod-{i % 10}",
        "destDomain": f"gmis-prod-{i % 10}",
        "oriMicroService": f"gateway-{i % 10}",
        "destMicroService": f"gateway-{i % 10}",
        "totalDuration": random.randint(6000, 10000),
        "retCode": random.randint(300, 500)
    }
    data.append(item)

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
