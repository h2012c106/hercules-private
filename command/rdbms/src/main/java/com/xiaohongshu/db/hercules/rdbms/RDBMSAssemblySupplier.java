package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
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

public class RDBMSAssemblySupplier extends BaseAssemblySupplier
        implements RDBMSManagerGenerator, DataTypeConverterGenerator<RDBMSDataTypeConverter> {

    @Override
    public DataSource getDataSource() {
        return new RDBMSDataSource();
    }

    @Override
    public OptionsConf getInputOptionsConf() {
        return new RDBMSInputOptionsConf();
    }

    @Override
    public OptionsConf getOutputOptionsConf() {
        return new RDBMSOutputOptionsConf();
    }

    @Override
    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return RDBMSInputFormat.class;
    }

    @Override
    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return RDBMSOutputFormat.class;
    }

    @Override
    public BaseSchemaFetcher<?> getSchemaFetcher() {
        return new RDBMSSchemaFetcher(options, generateConverter(), generateManager(options));
    }

    @Override
    public MRJobContext getJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return new RDBMSOutputMRJobContext();
    }

    @Override
    public RDBMSManager generateManager(GenericOptions options) {
        return new RDBMSManager(options);
    }

    @Override
    public RDBMSDataTypeConverter generateConverter() {
        return new RDBMSDataTypeConverter();
    }

    @Override
    public SchemaNegotiatorContext getSchemaNegotiatorContextAsSource() {
        return new RDBMSSchemaNegotiatorContext(options);
    }

    @Override
    public SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget() {
        return new RDBMSSchemaNegotiatorContext(options);
    }
}
