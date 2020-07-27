package com.xiaohongshu.db.hercules.clickhouse;

import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseInputFormat;
import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseOutputFormat;
import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseOutputMRContext;
import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseInputOptionsConf;
import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseOutputOptionsConf;
import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.clickhouse.schema.manager.ClickhouseManager;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class ClickhouseAssemblySupplier extends RDBMSAssemblySupplier {

    @Override
    public DataSource getDataSource() {
        return new ClickhouseDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new ClickhouseInputOptionsConf();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new ClickhouseOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return ClickhouseInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return ClickhouseOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new ClickhouseSchemaFetcher(options, generateConverter(), generateManager(options));
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new ClickhouseOutputMRContext();
    }

    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new ClickhouseManager(options);
    }
}
