package com.xiaohongshu.db.hercules.converter.blank;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public class BlankKvConverterSupplier implements KvConverterSupplier {

    @Override
    public KvConverter getKvConverter() {
        return new BlankKvConverter();

    }

    @Override
    public BaseOptionsConf getOptionsConf() {
        return new BlankOptionConf();
    }
}
