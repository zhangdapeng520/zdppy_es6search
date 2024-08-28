import json
from es6.elasticsearch import Elasticsearch
from log import logger

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


@logger.catch
def get_agg_buckets(result, agg_name, has_buckets=False):
    """提取聚合结果"""
    data = result.get("aggregations").get(agg_name)
    if has_buckets:
        return data.get("buckets")
    return data


def write_json(result):
    with open("test.json", "w") as f:
        json.dump(result, f)


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
agg_name = "log_level"
query = {
    "size": 0,
    "aggs": {
        # 聚合名称
        agg_name: {
            "filters": {
                # 没有被筛选到的分到此组
                "other_bucket_key": "other",
                "filters": {
                    # 根据match匹配结果进行分组
                    "info": {"match": {"body": "info"}},
                    "warning": {"match": {"body": "warning"}},
                }
            }
        }
    },
}
r = es.search(index, index, query)
write_json(r)

# 删除索引
es.indices.delete(index)
