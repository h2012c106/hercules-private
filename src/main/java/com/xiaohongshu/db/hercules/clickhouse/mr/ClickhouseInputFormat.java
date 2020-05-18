package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSFastSplitterGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.input.SplitGetter;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class ClickhouseInputFormat extends RDBMSInputFormat {
    @Override
    protected RDBMSSchemaFetcher initializeSchemaFetcher(GenericOptions options, RDBMSDataTypeConverter converter, RDBMSManager manager) {
        return new ClickhouseSchemaFetcher(options, converter, manager);
    }

    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return super.generateManager(options);
    }

    @Override
    protected SplitGetter getSplitGetter(GenericOptions options) {
        if (options.getBoolean(RDBMSInputOptionsConf.BALANCE_SPLIT, true)) {
            return new ClickhouseBalanceSplitGetter();
        } else {
            return new RDBMSFastSplitterGetter();
        }
    }
}
