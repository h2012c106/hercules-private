package com.xiaohongshu.db.hercules.clickhouse;

import com.xiaohongshu.db.hercules.clickhouse.input.mr.ClickhouseInputFormat;
import com.xiaohongshu.db.hercules.clickhouse.output.mr.ClickhouseOutputFormat;
import com.xiaohongshu.db.hercules.clickhouse.output.mr.ClickhouseOutputMRContext;
import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;

public class ClickhouseAssemblySupplier extends RDBMSAssemblySupplier {
    public ClickhouseAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return ClickhouseInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return ClickhouseOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return SchemaFetcherFactory.getSchemaFetcher(options, ClickhouseSchemaFetcher.class);
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new ClickhouseOutputMRContext();
    }
}
