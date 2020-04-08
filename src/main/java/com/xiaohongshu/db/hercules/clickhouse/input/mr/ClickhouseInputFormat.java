package com.xiaohongshu.db.hercules.clickhouse.input.mr;

import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.input.mr.RDBMSFastSplitterGetter;
import com.xiaohongshu.db.hercules.rdbms.input.mr.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.input.mr.SplitGetter;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;

public class ClickhouseInputFormat extends RDBMSInputFormat {
    @Override
    public RDBMSSchemaFetcher innerGetSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, ClickhouseSchemaFetcher.class);
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
