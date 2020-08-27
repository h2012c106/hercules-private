package com.xiaohongshu.db.hercules.parquetschema;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;

public class ParquetSchemaDataSource implements DataSource {
    @Override
    public String name() {
        return "ParquetSchema";
    }

    @Override
    public String getFilePositionParam() {
        return ParquetOptionsConf.DIR;
    }
}
