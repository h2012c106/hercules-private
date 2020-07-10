package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.SchemaNegotiatorContext;

public abstract class BaseAssemblySupplier implements AssemblySupplier {
    protected GenericOptions options;

    public BaseAssemblySupplier() {
    }

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    protected void afterSetOptions() {
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
