package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;

public class ClickhouseOutputMRContext extends RDBMSOutputMRJobContext {
    @Override
    public RDBMSSchemaFetcher getSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, ClickhouseSchemaFetcher.class);
    }
}
