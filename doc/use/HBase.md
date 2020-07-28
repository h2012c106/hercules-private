# HBase
## Source
支持基本数据类型，以及根据rowkey，版本号筛选需要导出的数据。
### 参数

| 参数名(实际使用需加上`source-`前缀) | 需要值 | 必填 | 默认值 | 描述                                                         |           例            |
| :---------------------------------: | :----: | :--: | :----: | ------------------------------------------------------------ | :---------------------: |
|               column                |   ✓    |  ✓   |   无   | 需要同步的列名列表，以`,`分隔                                |    `columnA,columnB`    |
|             column-type             |   ✓    |  ✓   |  `{}`  | 列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#datatype))，大小写不敏感 | `{"columnA":"integer"}` |
|          zookeeper-quorum           |   ✓    |  ✓   |   无   | 连接zookeeper                                                |                         |
|           zookeeper-port            |   ✓    |  ✗   |  2181  | 连接zookeeper                                                |         `2182`          |
|             hbase-table             |   ✓    |  ✓   |   无   | 需要导出的table名                                            |                         |
|            rowkeycolname            |   ✓    |  ✓   |   无   | 指定rowkey对应的列名                                         |                         |
|              row-start              |   ✓    |  ✗   |   无   | 指定起始rowkey，默认table最开始的rowkey                      |                         |
|              row-stop               |   ✓    |  ✗   |   无   | 指定终止rowkey，默认table最后的rowkey                        |                         |
|            column-family            |   ✓    |  ✓   |   无   | 指定table的column family                                     |          `cf`           |
|           scan-timestamp            |   ✓    |  ✗   |   无   | 指定特定版本                                                 |                         |
|           timerange-start           |   ✓    |  ✗   |   无   | 指定版本范围 start                                           |                         |
|            timerange-end            |   ✓    |  ✗   |   无   | 指定版本范围 end                                             |                         |
|           scan-cachedrows           |   ✓    |  ✗   |   无   | scanner 缓存条数                                             |                         |
|           scan-batchsize            |   ✓    |  ✗   |   无   | 每次返回的数据条数                                           |                         |

注意: 
+ 日期（Date，DateTime，Time）等默认存储为String或者Long，指定数据时直接指定为String或者Long等。
+ 最小split单位为一个region。 
+ 可以通过ColumnMap将短列名映射成有意义的列名传入下游（若下游不是HBase）。

TODO: 
+ 实现hive外表获取schema信息

### 性能


## Target
支持将对应的数据类型转成byte数组，异步多线程写入。
### 参数

| 参数名(实际使用需加上`source-`前缀) | 需要值 | 必填 |     默认值      | 描述                                                         |           例            |
| :---------------------------------: | :----: | :--: | :-------------: | ------------------------------------------------------------ | :---------------------: |
|               column                |   ✓    |  ✓   |       无        | 需要同步的列名列表，以`,`分隔                                |    `columnA,columnB`    |
|             column-type             |   ✓    |  ✓   |      `{}`       | 列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#datatype))，大小写不敏感 | `{"columnA":"integer"}` |
|          zookeeper-quorum           |   ✓    |  ✓   |       无        | 连接zookeeper                                                |                         |
|           zookeeper-port            |   ✓    |  ✗   |      2181       | 连接zookeeper                                                |         `2182`          |
|             hbase-table             |   ✓    |  ✓   |       无        | 需要导出的table名                                            |                         |
|            rowkeycolname            |   ✓    |  ✓   |       无        | 指定rowkey对应的列名                                         |                         |
|            column-family            |   ✓    |  ✓   |       无        | 指定table的column family                                     |          `cf`           |
|             max-threads             |   ✓    |  ✗   |        5        | 指定写入的线程数                                             |                         |
|           writebuffersize           |   ✓    |  ✗   | 8 * 1024 * 1024 | 写入缓存大小                                                 |                         |


注意:  
+ 若source不是HBase，强烈建议通过ColumnMap将长列名映射成短列名存储到HBase中。

### 性能