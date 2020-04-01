package com.xiaohongshu.db.hercules.mysql.input.mr;

import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.mysql.schema.MysqlSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.input.mr.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;

public class MysqlInputFormat extends RDBMSInputFormat {
    @Override
    public RDBMSSchemaFetcher innerGetSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, MysqlSchemaFetcher.class);
    }
}
