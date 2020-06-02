package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public interface HBaseManagerInitializer {
    HBaseManager initializeManager(GenericOptions options);
}
