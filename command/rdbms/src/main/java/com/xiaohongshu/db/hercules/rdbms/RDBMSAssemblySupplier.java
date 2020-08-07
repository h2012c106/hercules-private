package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
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
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManagerGenerator;

public class RDBMSAssemblySupplier extends BaseAssemblySupplier implements RDBMSManagerGenerator {

    @Override
    public DataSource innerGetDataSource() {
        return new RDBMSDataSource();
    }

    @Override
    public OptionsConf innerGetInputOptionsConf() {
        return new RDBMSInputOptionsConf();
    }

    @Override
    public OptionsConf innerGetOutputOptionsConf() {
        return new RDBMSOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat<?>> innerGetInputFormatClass() {
        return RDBMSInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat<?>> innerGetOutputFormatClass() {
        return RDBMSOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> innerGetSchemaFetcher() {
        return new RDBMSSchemaFetcher(options, (RDBMSDataTypeConverter) getDataTypeConverter(), generateManager(options));
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new RDBMSDataTypeConverter();
    }

    @Override
    public MRJobContext innerGetJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public MRJobContext innerGetJobContextAsTarget() {
        return new RDBMSOutputMRJobContext();
    }

    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new RDBMSManager(options);
    }

    @Override
    public SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsSource() {
        return new RDBMSSchemaNegotiatorContext(options);
    }

    @Override
    public SchemaNegotiatorContext innerGetSchemaNegotiatorContextAsTarget() {
        return new RDBMSSchemaNegotiatorContext(options);
    }
}
