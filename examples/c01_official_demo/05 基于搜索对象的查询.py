# 05 基于搜索对象的查询

import es6search

ess = es6search.new_with()
index = "user"
r = ess.match(index, "intro", "爬山")
print(r)
