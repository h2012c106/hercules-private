# 快速上手
## 工具用途
在两种异构数据源之间进行表级别的同步，默认通过列名进行大小写敏感的列的一一对应。  
## 命令格式
```
hadoop jar hercules-dev.jar [源数据源]::[目标数据源] [GENERIC_OPTIONS] [SOURCE_OPTIONS] [TARGET_OPTIONS] [COMMON_OPTIONS]
```
+ 源数据源: 源数据源[名字](../Introduction.md#概览)，大小写不敏感。
+ 目标数据源: 目标数据源[名字](../Introduction.md#概览)，大小写不敏感。
+ GENERIC_OPTIONS: Hadoop参数，例`-Dmapreduce.job.running.map.limit=2`。
+ SOURCE_OPTIONS: 源数据源参数，以`source-`前缀作为区分，例`--source-table 'test'`。
+ TARGET_OPTIONS: 目标数据源参数，以`target-`前缀作为区分，例`--target-table 'test'`。
+ COMMON_OPTIONS: 公共参数，无前缀，主要用于配置Hercules，例`--num-mapper 4`。

样例:
```
hadoop jar hercules-dev.jar TIDB::Parquet --source-connection 'jdbc:mysql://10.4.45.160:4000/sqoop_test' --source-user 'root' --source-password '******' --source-table 'obd_allocation_specify' --source-fetch-size 10000 --source-balance --source-split-by sys_no --target-dir 'obs://xhs.hw.tidb/hercules-test/' --target-schema-style sqoop --target-delete-target-dir --num-mapper 16 --allow-copy-column-name --allow-copy-column-type
```

## 强制指定的hadoop参数

|参数名|值|理由|
|:---:|:---:|---|
|`mapreduce.map.maxattempts` `mapred.map.max.attempts`|1|不允许map task失败重试，否则大概率导致数据重复|
|`mapreduce.map.speculative` `mapred.map.tasks.speculative.execution`|false|不允许speculative，理由同上|

## 公共参数

|参数名|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|help|✗|✗| |打印帮助信息| |
|num-mapper|✓|✗|`4`|map task的数量，控制并行度| |
|log-level|✓|✗|`INFO`|日志打印级别，全大写，`OFF`/`SEVERE`/`WARNING`/`INFO`/`CONFIG`/`FINE`/`FINER`/`FINEST`/`ALL`| |
|allow-source-more-column|✗|✗| |在源目标均可以获得列名列表时，允许源数据源表多列，详情参见[此处](../dev/Core.md#schemanegotiator)| |
|allow-target-more-column|✗|✗| |在源目标均可以获得列名列表时，允许目标数据源表多列，详情参见[此处](../dev/Core.md#schemanegotiator)| |
|column-map|✓|✗|`{}`|列名映射关系，json格式，key为源表列名，value为目标表列名，需要遵从BiMap规则，value不允许重复|`{"source_column": "target_column"}`|
|max-write-qps|✓|✗|无|极限写qps，若不指定，则不做限制。**注意: 若下游写使用batch形式，此参数仅会限制写batch缓存速度，仅能保证全局qps，若要压下瞬时写压力，需要同时考虑减小batch大小**。| |
|allow-copy-column-name|✗|✗| |是否允许上下游互相借鉴列名列表，详情参见[此处](../dev/Core.md#schemanegotiator)| |
|allow-copy-column-type|✗|✗| |是否允许上下游互相借鉴列类型映射，详情参见[此处](../dev/Core.md#schemanegotiator)| |

## 支持的数据类型
+ [RDBMS](./RDBMS.md)
+ [Mysql](./Mysql.md)
+ [TiDB](./TiDB.md)
+ [Clickhouse](./Clickhouse.md)
+ [MongoDB](./MongoDB.md)
+ [Parquet](./Parquet.md)
+ [ParquetSchema](./ParquetSchema.md)