package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public abstract class KvConverterSupplier<T, R, K, L> {

    private final KvConverter<T, R, K, L> kvConverter;
    private final BaseOptionsConf outputOptionsConf;
    private final BaseOptionsConf inputOptionsConf;

    public KvConverterSupplier(KvConverter<T, R, K, L> kvConverter, BaseOptionsConf outputOptionsConf, BaseOptionsConf inputOptionsConf) {
        this.kvConverter = kvConverter;
        this.outputOptionsConf = outputOptionsConf;
        this.inputOptionsConf = inputOptionsConf;
    }

    public KvConverter<T, R, K, L> getKvConverter(){
        return kvConverter;
    }

    public BaseOptionsConf getOutputOptionsConf(){
        return outputOptionsConf;
    }

    public BaseOptionsConf getInputOptionsConf(){
        return inputOptionsConf;
    }
}
