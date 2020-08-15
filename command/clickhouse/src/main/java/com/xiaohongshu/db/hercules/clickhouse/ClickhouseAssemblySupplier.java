package com.xiaohongshu.db.hercules.clickhouse;

import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseInputFormat;
import com.xiaohongshu.db.hercules.clickhouse.mr.ClickhouseOutputFormat;
import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseInputOptionsConf;
import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseOutputOptionsConf;
import com.xiaohongshu.db.hercules.clickhouse.schema.ClickhouseSchemaFetcher;
import com.xiaohongshu.db.hercules.clickhouse.schema.manager.ClickhouseManager;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class ClickhouseAssemblySupplier extends RDBMSAssemblySupplier {

    @Override
    protected DataSource innerGetDataSource() {
        return new ClickhouseDataSource();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new ClickhouseInputOptionsConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new ClickhouseOutputOptionsConf();
    }

    @Override
    protected Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return ClickhouseInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return ClickhouseOutputFormat.class;
    }

    @Override
    protected SchemaFetcher innerGetSchemaFetcher() {
        return new ClickhouseSchemaFetcher(options);
    }

    @Override
    protected RDBMSManager innerGetManager() {
        return new ClickhouseManager(options);
    }
}
