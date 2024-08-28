import unittest

import es6search

from es6.elasticsearch import Elasticsearch


class TestSearch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.es = Elasticsearch(
            hosts=["http://192.168.234.129:9200"],
            http_auth=("elastic", "zhangdapeng520"),
        )
        cls.ess = es6search.new(cls.es)

        # 创建索引
        cls.index = "user"
        mappings = {
            "mappings": {
                "user": {
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "ename": {"type": "keyword"},
                        "age": {"type": "integer"},
                        "intro": {"type": "text"},
                        "address": {
                            "type": "nested",
                            "properties": {
                                "province": {"type": "keyword"},
                                "city": {"type": "keyword"},
                            }
                        }
                    }
                }
            }
        }
        cls.es.indices.create(cls.index, mappings)

        # 新增
        data = [
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "1"}},
            {"id": 1, "name": "张三1", "ename": "zs1", "age": 23, "intro": "喜欢爬山，喜欢运动，喜欢一个人去旅行"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "2"}},
            {"id": 2, "name": "张三2", "ename": "zs2", "age": 23, "intro": "宅家程序员，喜欢每天看看书，写写代码"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "3"}},
            {"id": 3, "name": "张三3", "ename": "zs3", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "4"}},
            {"id": 4, "name": "zhangsan4", "ename": "zs4", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "5"}},
            {"id": 5, "name": "zhangsan love555", "ename": "zs4", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "6"}},
            {"id": 6, "name": "lisi", "ename": "李四", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "7"}},
            {"id": 7, "name": "李四", "ename": "lisi", "age": 23, "intro": "喜欢玩游戏，特别是电竞游戏，尤其喜欢玩英雄联盟和王者荣耀"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "8"}},
            {"id": 8, "name": "王五", "ename": "wangwu", "age": 25, "intro": "呵呵", "gender": "男"},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "9"}},
            {"id": 9, "name": "赵六", "ename": "zhaoliu", "age": 36, "intro": "老6",
             "address": {"province": "四川", "city": "成都"}},
            {"index": {"_index": cls.index, "_type": cls.index, "_id": "10"}},
            {"id": 10, "name": "赵六10", "ename": "zhaoliu10", "age": 36, "intro": "老6 10",
             "address": {"province": "北京", "city": "北京"}},
        ]
        cls.es.bulk(data)
        cls.es.indices.refresh(cls.index)

    def test_page(self):
        """测试分页查询"""
        r = es6search.page(self.es, self.index, 0, 2)
        print(r)

    def test_page2(self):
        """测试分页查询"""
        r = self.ess.page(self.index, 0, 2)
        print(r)

    def test_match(self):
        r = es6search.match(self.es, self.index, "intro", "爬山")
        print(r)

    def test_match2(self):
        r = self.ess.match(self.index, "intro", "爬山")
        print(r)

    def test_match_phrase(self):
        r = es6search.match_phrase(self.es, self.index, "intro", "爬山")
        print(r)

    def test_match_phrase2(self):
        r = self.ess.match_phrase(self.index, "intro", "爬山")
        print(r)

    def test_term(self):
        r = es6search.term(self.es, self.index, "ename", "zs1")
        print(r)
        r = es6search.term(self.es, self.index, "name", "张三2", is_text=True)
        print(r)

    def test_term2(self):
        r = self.ess.term(self.index, "ename", "zs1")
        print(r)
        r = self.ess.term(self.index, "name", "张三2", is_text=True)
        print(r)
        r = self.ess.term(self.index, "age", 23)
        print(r)

    def test_terms(self):
        r = es6search.terms(self.es, self.index, "ename", ["zs1"])
        print(r)
        r = self.ess.terms(self.index, "ename", ["zs1"])
        print(r)

    def test_multi_match(self):
        r = es6search.multi_match(self.es, self.index, "李四", ["name", "ename"])
        print(r)
        r = self.ess.multi_match(self.index, "李四", ["name", "ename"])
        print(r)

    def test_prefix(self):
        key = "ename"
        value = "李"
        r = es6search.prefix(self.es, self.index, key, value)
        print(r)
        r = self.ess.prefix(self.index, key, value)
        print(r)

    def test_wildcard(self):
        key = "ename"
        value = "李?"
        r = es6search.wildcard(self.es, self.index, key, value)
        print(r)
        r = self.ess.wildcard(self.index, key, value)
        print(r)

    def test_regexp(self):
        key = "ename"
        value = "zs[1-3]"
        r = es6search.regexp(self.es, self.index, key, value)
        print(r)
        r = self.ess.regexp(self.index, key, value)
        print(r)

    def test_bool_must(self):
        queries = [
            {"term": {"age": 23}},
            {"regexp": {"ename": "zs[1-2]"}}
        ]
        r = es6search.bool_must(self.es, self.index, queries)
        print(r)
        r = self.ess.bool_must(self.index, queries)
        print(r)

    def test_bool_should(self):
        queries = [
            {"term": {"age": 23}},
            {"regexp": {"ename": "zs[1-2]"}}
        ]
        r = es6search.bool_should(self.es, self.index, queries)
        print(r)
        r = self.ess.bool_should(self.index, queries)
        print(r)

    def test_bool_must_not(self):
        queries = [
            {"term": {"age": 23}},
            {"regexp": {"ename": "zs[1-2]"}}
        ]
        r = es6search.bool_must_not(self.es, self.index, queries)
        print(r)
        r = self.ess.bool_must_not(self.index, queries)
        print(r)

    def test_exists(self):
        r = es6search.exists(self.es, self.index, "gender")
        print(r)
        r = self.ess.exists(self.index, "gender")
        print(r)

    def test_range(self):
        r = es6search.range(self.es, self.index, "age", 24, 30)
        print(r)
        r = self.ess.range(self.index, "age", gte=25, lte=30)
        print(r)

    def test_nested(self):
        r = es6search.nested(self.es, self.index, "address", {"term": {"address.city": "成都"}})
        print(r)
        r = self.ess.nested(self.index, "address", {"term": {"address.city": "成都"}})
        print(r)

    @classmethod
    def tearDownClass(cls):
        cls.es.indices.delete(cls.index)
