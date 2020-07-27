package com.xiaohongshu.db.hercules.clickhouse;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class ClickhouseDataSource implements DataSource {
    @Override
    public String name() {
        return "Clickhouse";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
