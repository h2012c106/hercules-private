package com.xiaohongshu.db.hercules.hbase;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class HBaseDataSource implements DataSource {
    @Override
    public String name() {
        return "HBase";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
