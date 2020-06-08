# Parquet
## Source
### 参数

|参数名(实际使用需加上`source-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#datatype))，大小写不敏感|`{"columnA":"integer"}`|
|black-column|✓|✗|` `|不需要同步的列名列表，以`,`分隔，若和`column`有冲突，此配置更优先|`columnA,columnB`|
|dir|✓|✓| |parquet所在目录| |
|message-type|✓|✗|无|parquet schema| |
|schema-style|✓|✓| |schema类型，`SQOOP`/`HIVE`/`ORIGINAL`，较为一言难尽| |
|task-side-metadata|✗|✗| |是否在map task侧load schema，若是，能够加快切分，但是有倾斜的风险，不建议打开| |
|original-split|✗|✗| |是否使用Parquet自身的切分map task策略，若是，`num-mapper`参数将不起作用，虽然就算为否，mapper数也至少为文件数目| |
|empty-as-null|✗|✗| |控制optional的空值被当成空值还是null值| |

### 性能


## Target
### 参数

|参数名(实际使用需加上`target-`前缀)|需要值|必填|默认值|描述|例|
|:---:|:---:|:---:|:---:|---|:---:|
|column|✓|✗|无|需要同步的列名列表，以`,`分隔|`columnA,columnB`|
|column-type|✓|✗|`{}`|列类型，json格式，key为列名，大小写敏感；value为列类型([DataType](../dev/Core.md#datatype))，大小写不敏感|`{"columnA":"integer"}`|
|black-column|✓|✗|` `|不需要同步的列名列表，以`,`分隔，若和`column`有冲突，此配置更优先|`columnA,columnB`|
|dir|✓|✓| |parquet导出目录| |
|message-type|✓|✗|无|parquet schema| |
|schema-style|✓|✓| |schema类型，`SQOOP`/`HIVE`/`ORIGINAL`，较为一言难尽| |
|compression-codec|✓|✗|`SNAPPY`|输出parquet文件压缩算法，`UNCOMPRESSED`/`SNAPPY`/`GZIP`/`LZO`/`BROTLI`/`LZ4`/`ZSTD`| |
|delete-target-dir|✗|✗| |是否在写之前清空目标目录| |

注意:  
+ 若在map task之前无法生成/获得parquet schema(如mongodb)，map task将不由分说直接报错，为了优化此处体验，设计了[ParquetSchema](./ParquetSchema.md)功能，使得Hercules能够通过全量上游数据生成parquet schema，只用生成一次即可，生成后即可为此所用。

### 性能
