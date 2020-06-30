package com.xiaohongshu.db.hercules.parquet.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf;

public class ParquetInputParser extends BaseParser {

    public ParquetInputParser() {
        super(new ParquetInputOptionsConf(), DataSource.Parquet, DataSourceRole.SOURCE);
    }

}
