package com.xiaohongshu.db.hercules.serder.mongo;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.mongodb.mr.output.MongoDBWrapperSetterManager;

public class MongoOplogWrapperSetterManager extends MongoDBWrapperSetterManager {

    @Override
    public DataSourceRole getRole() {
        return DataSourceRole.SER;
    }
}
