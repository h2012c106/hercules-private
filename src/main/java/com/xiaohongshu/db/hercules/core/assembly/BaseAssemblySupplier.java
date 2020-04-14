package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;

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

    public BaseAssemblySupplier(GenericOptions options) {
        this.options = options;
        initialize();
        inputFormatClass = setInputFormatClass();
        outputFormatClass = setOutputFormatClass();
        schemaFetcher = setSchemaFetcher();
        jobContextAsSource = setJobContextAsSource();
        jobContextAsTarget = setJobContextAsTarget();
    }

    protected void initialize() {
    }

    public Class<? extends HerculesInputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    abstract protected Class<? extends HerculesInputFormat> setInputFormatClass();

    public Class<? extends HerculesOutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    abstract protected Class<? extends HerculesOutputFormat> setOutputFormatClass();

    public BaseSchemaFetcher getSchemaFetcher() {
        return schemaFetcher;
    }

    abstract protected BaseSchemaFetcher setSchemaFetcher();

    public MRJobContext getJobContextAsSource() {
        return jobContextAsSource;
    }

    abstract protected MRJobContext setJobContextAsSource();

    public MRJobContext getJobContextAsTarget() {
        return jobContextAsTarget;
    }

    abstract protected MRJobContext setJobContextAsTarget();
}
