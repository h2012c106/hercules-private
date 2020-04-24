package com.xiaohongshu.db.hercules.hbase2;

import com.xiaohongshu.db.hercules.core.assembly.BaseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.assembly.NullMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

public class HbaseAssemblySupplier extends BaseAssemblySupplier {

    public HbaseAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<HbaseInputFormat> setInputFormatClass() {
        return HbaseInputFormat.class;
    }

    @Override
    protected Class<HbaseOutputFormat> setOutputFormatClass() {
        return HbaseOutputFormat.class;
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return null;
    }

    @Override
    protected MRJobContext setJobContextAsSource() {
        return NullMRJobContext.INSTANCE;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new HbaseOutputMRJobContext();
    }
}
