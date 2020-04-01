package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * 子类必须向{@link AssemblySupplierFactory}中注册自己的类
 */
public abstract class BaseAssemblySupplier {
    private GenericOptions options;

    protected Class<? extends InputFormat> inputFormatClass;
    protected Class<? extends OutputFormat> outputFormatClass;
    protected BaseSchemaFetcher schemaFetcher;
    protected MRJobContext jobContext;

    abstract public DataSource getDataSource();

    public BaseAssemblySupplier(GenericOptions options) {
        this.options = options;
        initialize();
        setInputFormatClass();
        setOutputFormatClass();
        setSchemaFetcher();
        setJobContext();
    }

    protected void initialize() {
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    abstract protected void setInputFormatClass();

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    abstract protected void setOutputFormatClass();

    public BaseSchemaFetcher getSchemaFetcher() {
        return schemaFetcher;
    }

    abstract protected void setSchemaFetcher();

    public MRJobContext getJobContext() {
        return jobContext;
    }

    abstract protected void setJobContext();
}
