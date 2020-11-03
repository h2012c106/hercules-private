package com.xiaohongshu.db.hercules.bson.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.mongodb.mr.output.MongoDBWrapperSetterManager;

public class BsonWrapperSetterManager extends MongoDBWrapperSetterManager {
    public BsonWrapperSetterManager() {
        super(DataSourceRole.TARGET);
    }
}
