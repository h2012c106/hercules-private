package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class RDBMSDataSource implements DataSource {

    @Override
    public String name() {
        return "RDBMS";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
