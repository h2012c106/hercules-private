package com.xiaohongshu.db.hercules.mongodb;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class MongoDBDataSource implements DataSource {

    @Override
    public String name() {
        return "MongoDB";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }

}
