package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManagerGenerator;

public class RDBMSAssemblySupplier extends BaseAssemblySupplier
        implements RDBMSManagerGenerator, DataTypeConverterGenerator<RDBMSDataTypeConverter> {
    public RDBMSAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return RDBMSInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return RDBMSOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new RDBMSSchemaFetcher(options, generateConverter(), generateManager(options));
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
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
    protected SchemaNegotiatorContext setSchemaNegotiatorContextAsSource() {
        return new RDBMSSchemaNegotiatorContext(options);
    }

    @Override
    protected SchemaNegotiatorContext setSchemaNegotiatorContextAsTarget() {
        return new RDBMSSchemaNegotiatorContext(options);
    }
}
