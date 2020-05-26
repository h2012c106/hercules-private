# RDBMS
## Source
有较高灵活性，支持通过表名或SQL进行抽取。  
### 参数

|参数名(实际使用需加上`source-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#DataType))，大小写不敏感|`{"columnA":"integer"}`|
|black-column|✓|✗|` `|不需要同步的列名列表，以`,`分隔，若和`column`有冲突，此配置更优先|`columnA,columnB`|
|connection|✓|✓|无|数据库jdbc链接，需要带上database名|`jdbc:clickhouse://10.0.210.69:8123/sqoop_test`|
|user|✓|✗|无|数据库用户名，若不指定，则无用户名登录| |
|password|✓|✗|无|数据库密码，若不指定，则无密码登录| |
|driver|✓|✓|`ru.yandex.clickhouse.ClickHouseDriver`|数据库jdbc driver包名，必须指定|`com.mysql.jdbc.Driver`|
|table|✓|✗|无|抽取的table名| |
|condition|✓|✗|无|使用`table`时，额外的`where`语句筛选条件|`id > 100`|
|query|✓|✗|无|抽取sql。**若存在函数式，强烈建议使用`as`指定列名，否则无法在不指定`column`的情况下获得正常的列名。**|`select a+b as column_sum from tb;`|
|split-by|✓|✓| |用于切分map task的列名，**强烈建议加索引**| |
|balance|✗|✗| |是否平衡切分，若不指定则使用`split-by`列最大最小值切分，否则进行抽样获得n等分点，避免map倾斜| |
|random-func-name|✓|✗|`(RAND() / 4294967295)`|指定范围为\[0, 1]的随机函数，用于指定`balance`时| |
|balance-sample-max-row|✓|✗| |随机抽样的最大行数，用于优化内存占用| |

注意:  
+ `table`与`query`两者必须出现其一，且不能同时出现。
+ 若同时使用`query`与`column`，则其指定的`select`顺序一定要与`column`一致。
+ Clickhouse的balance模式会优先试图用`quatiles()`函数获得切分点，若列类型不允许，再使用随机抽样。

### 性能


## Target
支持多种写入模式、staging table以及多线程写入。   
### 参数

|参数名(实际使用需加上`target-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#DataType))，大小写不敏感|`{"columnA":"integer"}`|
|connection|✓|✓|无|数据库jdbc链接，需要带上database名|`jdbc:clickhouse://10.0.210.69:8123/sqoop_test`|
|user|✓|✗|无|数据库用户名，若不指定，则无用户名登录| |
|password|✓|✗|无|数据库密码，若不指定，则无密码登录| |
|driver|✓|✓|`ru.yandex.clickhouse.ClickHouseDriver`|数据库jdbc driver包名，必须指定|`com.mysql.jdbc.Driver`|
|table|✓|✗|无|目标table名| |
|export-type|✓|✓|无|写模式，`INSERT`/`UPDATE`/`INSERT_IGNORE`/`UPSERT`/`REPLACE`| |
|update-key|✓|✗|无|更新键，当`export-type`为`UPDATE`时，必须指定。允许指定多列，以`,`分隔|`updateKeyA,updateKeyB`|
|staging-table|✓|✗|无|同sqoop对应概念，但是写入姿势不同——sqoop是使用配置的`export-type`写入，随后`insert ... select ...`；而hercules在不配置`close-force-insert-staging`时，全部insert入staging表，随后按照`export-type`更新入目标表，理由见本章**注意**。若不指定，则不使用staging表。| |
|close-force-insert-staging|✗|✗| |允许对staging表做`export-type`行为，有风险，慎用。| |
|pre-migrate-sql|✓|✗|无|在map task导到staging table后，执行staging table合并操作前，执行的sql语句，减小目标表数据不可用时间。|`truncate table test`|
|batch|✗|✗| |是否使用jdbc `PreparedStatement`的batch写特性| |
|record-per-statement|✓|✗|100|每条sql内包含多少行，即batch大小| |
|execute-thread-num|✓|✗|1|并行写线程数| |
|enable-null|✗|✗| |是否允许直接插入null值，若不允许则采用[默认值](https://wiki.xiaohongshu.com/pages/viewpage.action?pageId=64870770)代替| |

注意:  
+ 未经配置应当仅支持向staging table insert，其他操作一概不允许，staging table只是一个源数据的数据库形态，不应有任何改动。如果向staging table做replace等依赖数据库键的操作，必须是staging table的键约束真低于目标表约束，一旦有多余的约束数据极其容易丢，然而这个东西本工具无法保证，那么仅允许insert staging table是必要的，这样就能变向约束staging表约束低于目标表。
+ update不允许staging table，显而易见。

### 性能
