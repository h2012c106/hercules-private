package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface HBaseManagerInitializer {
    HBaseManager initializeManager(GenericOptions options);
}
