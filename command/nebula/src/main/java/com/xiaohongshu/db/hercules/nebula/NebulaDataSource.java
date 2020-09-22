package com.xiaohongshu.db.hercules.nebula;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class NebulaDataSource implements DataSource {
    @Override
    public String name() {
        return "Nebula";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
