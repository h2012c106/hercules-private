# RDBMS
## Source
有较高灵活性，支持通过使用query动态查询。  
### 参数

|参数名(实际使用需加上`source-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#DataType))，大小写不敏感|`{"columnA":"integer"}`|
|black-column|✓|✗|` `|不需要同步的列名列表，以`,`分隔，若和`column`有冲突，此配置更优先|`columnA,columnB`|
|connection|✓|✓|无|mongodb链接，格式`<host>:<port>`，允许多个，以`,`分隔|`10.4.12.12:27018`|
|user|✓|✗|无|数据库用户名，若不指定，则无用户名登录| |
|password|✓|✗|无|数据库密码，若不指定，则无密码登录| |
|authdb|✓|✗|无|数据库authdb，若不指定，则使用`database`指定值| |
|database|✓|✓|无|同步表db名| |
|collection|✓|✓|无|同步表collection名| |
|query|✓|✗|无|抽取时的额外搜索条件|`{id: {$gte: 10}}`|
|split-by|✓|✗|`_id`|用于切分map task的列名，**强烈建议加索引**| |

注意: 
+ 一定要保证`split-by`列类型一致，不然可能会丢数据。

TODO: 
+ 允许`split-by`列多类型。

### 性能


## Target
支持多种写入模式、upsert以及多线程写入。   
### 参数

|参数名(实际使用需加上`target-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#DataType))，大小写不敏感|`{"columnA":"integer"}`|
|black-column|✓|✗|` `|不需要同步的列名列表，以`,`分隔，若和`column`有冲突，此配置更优先|`columnA,columnB`|
|connection|✓|✓|无|mongodb链接，格式`<host>:<port>`，允许多个，以`,`分隔|`10.4.12.12:27018`|
|user|✓|✗|无|数据库用户名，若不指定，则无用户名登录| |
|password|✓|✗|无|数据库密码，若不指定，则无密码登录| |
|authdb|✓|✗|无|数据库authdb，若不指定，则使用`database`指定值| |
|database|✓|✓|无|同步表db名| |
|collection|✓|✓|无|同步表collection名| |
|object-id|✓|✗|`_id`|由于工具[内部类型](../dev/Core.md#DataType)无`ObjectId`，需要显式指定，支持多列，以`,`分隔| |
|export-type|✓|✓|无|写模式，`INSERT`/`UPDATE_ONE`/`UPDATE_MANY`/`REPLACE_ONE`| |
|update-key|✓|✗|无|更新键，当`export-type`不为`INSERT`时，必须指定。允许指定多列，以`,`分隔|`updateKeyA,updateKeyB`|
|upsert|✗|✗| |若更新时，`update-key`下无数据，是否插入| |
|statement-per-bulk|✓|✗|200|每个提交内的行数，即batch大小| |
|[bulk-ordered](https://docs.mongodb.com/manual/core/bulk-write-operations/#ordered-vs-unordered-operations)|✗|✗| |提交姿势为顺序同步还是异步乱序(效率更高)| |
|execute-thread-num|✓|✗|1|写线程数| |
|decimal-as-string|✗|✗| |是否把decimal类型写成string类型，兼容mongo3.4以下的版本| |

注意:  
+ `upsert`不能和`export-type insert`共同使用，显而易见。

### 性能
