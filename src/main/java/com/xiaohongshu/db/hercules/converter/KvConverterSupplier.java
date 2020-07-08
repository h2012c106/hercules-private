package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public abstract class KvConverterSupplier {

    private KvConverter kvConverter;
    private BaseOptionsConf baseOptionsConf;

    public KvConverterSupplier(KvConverter kvConverter, BaseOptionsConf baseOptionsConf) {
        this.kvConverter = kvConverter;
        this.baseOptionsConf = baseOptionsConf;
    }

    public KvConverter getKvConverter(){
        return kvConverter;
    }

    public BaseOptionsConf getOptionsConf(){
        return baseOptionsConf;
    }
}
