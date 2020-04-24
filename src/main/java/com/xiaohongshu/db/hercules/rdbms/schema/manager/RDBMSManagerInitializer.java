package com.xiaohongshu.db.hercules.rdbms.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface RDBMSManagerInitializer {
    RDBMSManager initializeManager(GenericOptions options);
}
