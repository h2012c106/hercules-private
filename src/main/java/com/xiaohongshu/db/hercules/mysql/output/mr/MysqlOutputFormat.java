package com.xiaohongshu.db.hercules.mysql.output.mr;

import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.mysql.schema.MysqlSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.output.mr.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;

public class MysqlOutputFormat extends RDBMSOutputFormat {
    @Override
    public RDBMSSchemaFetcher innerGetSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, MysqlSchemaFetcher.class);
    }
}
