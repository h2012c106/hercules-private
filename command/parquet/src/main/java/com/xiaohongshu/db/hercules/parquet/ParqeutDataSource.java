package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;

public class ParqeutDataSource implements DataSource {

    @Override
    public String name() {
        return "Parqeut";
    }

    @Override
    public String getFilePositionParam() {
        return ParquetOptionsConf.DIR;
    }

}
