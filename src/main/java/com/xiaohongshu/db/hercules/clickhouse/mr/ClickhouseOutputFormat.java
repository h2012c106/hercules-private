package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.clickhouse.schema.manager.ClickhouseManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSWrapperSetterFactory;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

import static com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseOutputOptionsConf.ENABLE_NULL;

public class ClickhouseOutputFormat extends RDBMSOutputFormat {
    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new ClickhouseManager(options);
    }

    @Override
    protected RDBMSWrapperSetterFactory generateWrapperSetterFactory(GenericOptions targetOptions) {
        boolean enableNull = targetOptions.getBoolean(ENABLE_NULL, false);
        return new ClickhouseWrapperSetterFactory(enableNull);
    }
}
