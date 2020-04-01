package com.xiaohongshu.db.hercules.mysql;

import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.mysql.input.mr.MysqlInputFormat;
import com.xiaohongshu.db.hercules.mysql.output.mr.MysqlOutputFormat;
import com.xiaohongshu.db.hercules.mysql.output.mr.MysqlOutputMRJobContext;
import com.xiaohongshu.db.hercules.mysql.schema.MysqlSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;

public class MysqlAssemblySupplier extends RDBMSAssemblySupplier {
    public MysqlAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return MysqlInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return MysqlOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return SchemaFetcherFactory.getSchemaFetcher(options, MysqlSchemaFetcher.class);
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new MysqlOutputMRJobContext();
    }
}
