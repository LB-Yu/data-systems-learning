# PlayDB

PlayDB包含一个简单的用Antlr解析SQL的案例, 采用了SQLite的SQL语法文件. 它解析一段SQL并判断当前SQL应该在哪个子库中进行查询.

运行方式如下:
```
# 在antlr-learning/src/main/java/org/antlr/v4/examples/playdb/parser目录下执行
antlr4 -visitor -package org.antlr.v4.examples.playdb.parser SQLite.g4
```

最后运行`PlayDB.java`即可.