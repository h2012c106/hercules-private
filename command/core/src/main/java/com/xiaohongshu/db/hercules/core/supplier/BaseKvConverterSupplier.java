package com.xiaohongshu.db.hercules.core.supplier;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public abstract class BaseKvConverterSupplier implements KvConverterSupplier {

    protected GenericOptions options;

    @Override
    public void setOptions(GenericOptions options) {
        this.options = options;
        afterSetOptions();
    }

    protected void afterSetOptions() {
    }

}
