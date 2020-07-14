package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;

public interface AssemblySupplier {

    public void setOptions(GenericOptions options);

    public DataSource getDataSource();

    public OptionsConf getInputOptionsConf();

    public OptionsConf getOutputOptionsConf();

    public Class<? extends HerculesInputFormat> getInputFormatClass();

    public Class<? extends HerculesOutputFormat> getOutputFormatClass();

    public BaseSchemaFetcher<?> getSchemaFetcher();

    public MRJobContext getJobContextAsSource();

    public MRJobContext getJobContextAsTarget();

    public SchemaNegotiatorContext getSchemaNegotiatorContextAsSource();

    public SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget();
}
