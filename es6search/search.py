from es6.elasticsearch import Elasticsearch
from .page import page as _page
from .match import match as _match, match_phrase, multi_match
from .term import term as _term, terms
from .prefix import prefix
from .wildcard import wildcard
from .regexp import regexp
from ._bool import bool_must, bool_should, bool_must_not
from .exists import exists
from .range import range
from .nested import nested


class Search:
    """
    专门用于搜索的对象
    """

    def __init__(self, client):
        """
        :param client es6的客户端对象
        """
        self.client = client

    def page(self, index, offset=0, size=20, query=None, doc_type=None, is_source=True):
        """
        分页查询
        """
        return _page(self.client, index, offset, size, query, doc_type, is_source)

    def match(self, index, key, value, doc_type=None, is_source=True):
        """
        分词查询
        """
        return _match(self.client, index, key, value, doc_type, is_source)

    def match_phrase(self, index, key, value, doc_type=None, is_source=True):
        """
        短语查询
        """
        return match_phrase(self.client, index, key, value, doc_type, is_source)

    def term(self, index, key, value, doc_type=None, is_text=False, is_source=True):
        """
        关键字等值查询
        """
        return _term(self.client, index, key, value, doc_type, is_text, is_source)

    def terms(self, index, key, value, doc_type=None, is_text=False, is_source=True):
        """
        关键字等值查询
        :param value 应该是list类型，形成类似SQL的in查询
        """
        return terms(self.client, index, key, value, doc_type, is_text, is_source)

    def multi_match(self, index, key, value, doc_type=None, is_source=True):
        """
        多字段分词查询
        :param key 你要查询的关键字的值
        :param value 是列表类型，指定你要查询的列名
        """
        return multi_match(self.client, index, key, value, doc_type, is_source)

    def prefix(self, index, key, value, doc_type=None, is_text=False, is_source=True):
        """
        前缀查询，适用于keyword类型和text类型的字段
        :param key 你要查询的关键字的值
        :param value 你要查询的值，比如姓张的用户则输入“张”
        """
        return prefix(self.client, index, key, value, doc_type, is_text, is_source)

    def wildcard(self, index, key, value, doc_type=None, is_text=False, is_source=True):
        """
        通配符查看，可以使用?代替一个字符进行查询
        :param value 带通配符的值，必须是字符串类型，必须有通配符?或者*
        """
        return wildcard(self.client, index, key, value, doc_type, is_text, is_source)

    def regexp(self, index, key, value, doc_type=None, is_text=False, is_source=True):
        """
        正则查询
        """
        return regexp(self.client, index, key, value, doc_type, is_text, is_source)

    def bool_must(self, index, queries, doc_type=None, is_source=True):
        """
        多条件且查询
        """
        return bool_must(self.client, index, queries, doc_type, is_source)

    def bool_should(self, index, queries, doc_type=None, is_source=True):
        """
        多条件或查询
        """
        return bool_should(self.client, index, queries, doc_type, is_source)

    def bool_must_not(self, index, queries, doc_type=None, is_source=True):
        """
        多条件非查询
        """
        return bool_must_not(self.client, index, queries, doc_type, is_source)

    def exists(self, index, field, doc_type=None, is_source=True):
        """
        查询某列不为空的数据
        """
        return exists(self.client, index, field, doc_type, is_source)

    def range(self, index, key, gt=None, lt=None, gte=None, lte=None, doc_type=None, is_source=True):
        """
        范围查询
        """
        return range(self.client, index, key, gt, lt, gte, lte, doc_type, is_source)

    def nested(self, index, key, query, doc_type=None, is_source=True):
        """
        范围查询
        """
        return nested(self.client, index, key, query, doc_type, is_source)


def new(client):
    """
    新建搜索对象
    """
    return Search(client)


def new_with(
        host="localhost",
        port=9200,
        username="elastic",
        password="zhangdapeng520",
):
    """
    根据用户名和密码获取客户端
    :return: 客户端对象
    """
    client = Elasticsearch(
        hosts=[f"http://{host}:{port}"],
        http_auth=(username, password),
    )
    return new(client)
