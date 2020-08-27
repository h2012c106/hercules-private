package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;

public class ParquetDataSource implements DataSource {

    @Override
    public String name() {
        return "Parquet";
    }

    @Override
    public String getFilePositionParam() {
        return ParquetOptionsConf.DIR;
    }

}
