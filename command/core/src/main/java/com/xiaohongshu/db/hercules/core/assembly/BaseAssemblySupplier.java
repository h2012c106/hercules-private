package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;

/**
 * 子类必须向{@link AssemblySupplierFactory}中注册自己的类
 */
public abstract class BaseAssemblySupplier {
    protected GenericOptions options;

    protected Class<? extends HerculesInputFormat> inputFormatClass;
    protected Class<? extends HerculesOutputFormat> outputFormatClass;
    protected BaseSchemaFetcher schemaFetcher;
    protected MRJobContext jobContextAsSource;
    protected MRJobContext jobContextAsTarget;
    protected SchemaNegotiatorContext schemaNegotiatorContextAsSource;
    protected SchemaNegotiatorContext schemaNegotiatorContextAsTarget;

    public BaseAssemblySupplier(GenericOptions options) {
        this.options = options;
        initialize();
        inputFormatClass = setInputFormatClass();
        outputFormatClass = setOutputFormatClass();
        schemaFetcher = setSchemaFetcher();
        jobContextAsSource = setJobContextAsSource();
        jobContextAsTarget = setJobContextAsTarget();
        schemaNegotiatorContextAsSource = setSchemaNegotiatorContextAsSource();
        schemaNegotiatorContextAsTarget = setSchemaNegotiatorContextAsTarget();
    }

    protected void initialize() {
    }

    public final Class<? extends HerculesInputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    abstract protected Class<? extends HerculesInputFormat> setInputFormatClass();

    public final Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    abstract protected Class<? extends HerculesOutputFormat> setOutputFormatClass();

    public final BaseSchemaFetcher getSchemaFetcher() {
        return schemaFetcher;
    }

    abstract protected BaseSchemaFetcher setSchemaFetcher();

    public final MRJobContext getJobContextAsSource() {
        return jobContextAsSource;
    }

    abstract protected MRJobContext setJobContextAsSource();

    public final MRJobContext getJobContextAsTarget() {
        return jobContextAsTarget;
    }

    abstract protected MRJobContext setJobContextAsTarget();

    public final SchemaNegotiatorContext getSchemaNegotiatorContextAsSource() {
        return schemaNegotiatorContextAsSource;
    }

    protected SchemaNegotiatorContext setSchemaNegotiatorContextAsSource() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    public final SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget() {
        return schemaNegotiatorContextAsTarget;
    }

    protected SchemaNegotiatorContext setSchemaNegotiatorContextAsTarget() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }
}
