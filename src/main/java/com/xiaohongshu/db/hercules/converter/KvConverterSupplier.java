package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public interface KvConverterSupplier {

    KvConverter getKvConverter();
    BaseOptionsConf getOptionsConf();
}
