package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.mr.context.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.SchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;

/**
 * 千万保证模块间别出循环依赖，出了就stack overflow，事前根本没法检查
 */
public interface AssemblySupplier {

    public void setOptions(GenericOptions options);

    public DataSource getDataSource();

    public OptionsConf getInputOptionsConf();

    public OptionsConf getOutputOptionsConf();

    public Class<? extends HerculesInputFormat<?>> getInputFormatClass();

    public Class<? extends HerculesOutputFormat<?>> getOutputFormatClass();

    public SchemaFetcher getSchemaFetcher();

    public MRJobContext getJobContextAsSource();

    public MRJobContext getJobContextAsTarget();

    public SchemaNegotiatorContext getSchemaNegotiatorContextAsSource();

    public SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget();

    public DataTypeConverter<?,?> getDataTypeConverter();

    public CustomDataTypeManager<?,?> getCustomDataTypeManager();

}
