# Parquet
## Target
支持从源数据源表的全量数据生成一张parquet schema，支持多map。  
### 参数

|参数名(实际使用需加上`target-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#DataType))，大小写不敏感|`{"columnA":"integer"}`|
|black-column|✓|✗|` `|不需要同步的列名列表，以`,`分隔，若和`column`有冲突，此配置更优先|`columnA,columnB`|
|dir|✓|✓| |parquet导出目录| |
|message-type|✓|✗|无|parquet schema| |
|schema-style|✓|✓| |schema类型，`SQOOP`/`HIVE`/`ORIGINAL`，较为一言难尽| |
|try-required|✗|✗| |尽可能地将列标记为`required`，否则默认都为`optional`| |
|type-auto-upgrade|✗|✗| |当两同名列类型不一致时，试图将精度低类型转换为高精度类型，如`INTEGER`->`LONG`| |

### 性能
