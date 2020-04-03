package com.xiaohongshu.db.hercules.clickhouse.schema.manager;

import com.xiaohongshu.db.hercules.clickhouse.input.options.ClickhouseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class ClickhouseManager extends RDBMSManager {
    public ClickhouseManager(GenericOptions options) {
        super(options);
    }

    @Override
    public String getRandomFunc() {
        return options.getString(RDBMSInputOptionsConf.RANDOM_FUNC_NAME, ClickhouseInputOptionsConf.DEFAULT_RANDOM_FUNC_NAME);
    }
}
