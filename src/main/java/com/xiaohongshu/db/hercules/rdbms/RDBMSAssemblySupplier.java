package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.assembly.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterInitializer;
import com.xiaohongshu.db.hercules.hbase2.HbaseOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManagerInitializer;

public class RDBMSAssemblySupplier extends BaseAssemblySupplier
        implements RDBMSManagerInitializer, DataTypeConverterInitializer<RDBMSDataTypeConverter> {
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
        return new RDBMSSchemaFetcher(options, initializeConverter(), initializeManager(options));
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
    public RDBMSManager initializeManager(GenericOptions options) {
        return new RDBMSManager(options);
    }

    @Override
    public RDBMSDataTypeConverter initializeConverter() {
        return new RDBMSDataTypeConverter();
    }
}
