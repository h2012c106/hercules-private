package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.assembly.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.input.mr.RDBMSBalanceSplitGetter;
import com.xiaohongshu.db.hercules.rdbms.input.mr.RDBMSFastSplitterGetter;
import com.xiaohongshu.db.hercules.rdbms.input.mr.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.mr.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.output.mr.RDBMSOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

public class RDBMSAssemblySupplier extends BaseAssemblySupplier {
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
        return SchemaFetcherFactory.getSchemaFetcher(options, RDBMSSchemaFetcher.class);
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new RDBMSOutputMRJobContext();
    }
}
