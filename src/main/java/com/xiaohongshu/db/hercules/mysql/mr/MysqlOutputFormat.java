package com.xiaohongshu.db.hercules.mysql.mr;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.mysql.schema.MysqlSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;

public class MysqlOutputFormat extends RDBMSOutputFormat {
    @Override
    public RDBMSSchemaFetcher innerGetSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, MysqlSchemaFetcher.class);
    }
}
