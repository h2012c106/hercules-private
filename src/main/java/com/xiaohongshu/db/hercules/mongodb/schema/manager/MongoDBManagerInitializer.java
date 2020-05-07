package com.xiaohongshu.db.hercules.mongodb.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface MongoDBManagerInitializer {
    MongoDBManager initializeManager(GenericOptions options);
}
