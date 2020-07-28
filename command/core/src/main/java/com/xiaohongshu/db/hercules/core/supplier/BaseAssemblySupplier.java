package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;

public abstract class BaseAssemblySupplier implements AssemblySupplier {
    protected GenericOptions options;

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    protected void afterSetOptions() {
    }

    @Override
    public MRJobContext getJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public MRJobContext getJobContextAsTarget() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    public SchemaNegotiatorContext getSchemaNegotiatorContextAsSource() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

    @Override
    public SchemaNegotiatorContext getSchemaNegotiatorContextAsTarget() {
        return SchemaNegotiatorContext.NULL_INSTANCE;
    }

}
