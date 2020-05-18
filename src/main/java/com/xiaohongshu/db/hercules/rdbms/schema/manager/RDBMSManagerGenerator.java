package com.xiaohongshu.db.hercules.rdbms.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface RDBMSManagerGenerator {
    RDBMSManager generateManager(GenericOptions options);
}
