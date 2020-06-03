package com.xiaohongshu.db.hercules.clickhouse;

import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseInputFormat;
import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseOutputFormat;
import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseOutputMRContext;
import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.clickhouse.schema.manager.ClickhouseManager;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class ClickhouseAssemblySupplier extends RDBMSAssemblySupplier {
    public ClickhouseAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return ClickhouseInputFormat.class;
    }

    @Override
    protected Class<ClickhouseOutputFormat> setOutputFormatClass() {
        return ClickhouseOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new ClickhouseSchemaFetcher(options, generateConverter(), generateManager(options));
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new ClickhouseOutputMRContext();
    }

    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new ClickhouseManager(options);
    }
}
