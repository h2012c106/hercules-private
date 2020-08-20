package com.xiaohongshu.db.hercules.parquetschema.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf;

public class ParquetSchemaParser extends BaseParser {

    public ParquetSchemaParser() {
        super(new ParquetSchemaOptionsConf(), DataSource.ParquetSchema, DataSourceRole.TARGET);
    }

}