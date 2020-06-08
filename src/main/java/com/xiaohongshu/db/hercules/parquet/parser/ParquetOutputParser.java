package com.xiaohongshu.db.hercules.parquet.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf;

public class ParquetOutputParser extends BaseParser {

    public ParquetOutputParser() {
        super(new ParquetOutputOptionsConf(), DataSource.Parquet, DataSourceRole.TARGET);
    }

}
