package com.xiaohongshu.db.hercules.hbase;

import com.xiaohongshu.db.hercules.core.supplier.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseInputFormat;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseOutputFormat;
import com.xiaohongshu.db.hercules.hbase.mr.HBaseOutputMRJobContext;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseSchemaFetcher;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;

public class HBaseAssemblySupplier extends BaseAssemblySupplier
        implements HBaseManagerInitializer, DataTypeConverterGenerator<HBaseDataTypeConverter> {

    @Override
    public DataSource getDataSource() {
        return new HBaseDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new HBaseInputOptionsConf();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new HBaseOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return HBaseInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return HBaseOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new HBaseSchemaFetcher(options, generateConverter());
    }

    @Override
    public MRJobContext getJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new HBaseOutputMRJobContext();
    }

    @Override
    public SchemaNegotiatorContext getSchemaNegotiatorContextAsSource() {
        return new HBaseSchemaNegotiatorContext(options);
    }

    @Override
    public SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget() {
        return new HBaseSchemaNegotiatorContext(options);
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }

    @Override
    public HBaseDataTypeConverter generateConverter() {
        return new HBaseDataTypeConverter();
    }

}

