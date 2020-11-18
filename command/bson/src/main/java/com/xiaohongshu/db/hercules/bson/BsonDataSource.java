package com.xiaohongshu.db.hercules.bson;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class BsonDataSource implements DataSource {

    @Override
    public String name() {
        return "Bson";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
