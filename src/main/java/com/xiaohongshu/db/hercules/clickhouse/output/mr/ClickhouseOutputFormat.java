package com.xiaohongshu.db.hercules.clickhouse.output.mr;

import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.output.mr.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;

public class ClickhouseOutputFormat extends RDBMSOutputFormat {
    @Override
    public RDBMSSchemaFetcher innerGetSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, ClickhouseSchemaFetcher.class);
    }
}
