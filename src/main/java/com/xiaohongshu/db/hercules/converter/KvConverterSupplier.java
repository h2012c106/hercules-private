package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public abstract class KvConverterSupplier {

    private KvConverter kvConverter;
    private BaseOptionsConf outputOptionsConf;
    private BaseOptionsConf inputOptionsConf;

    public KvConverterSupplier(KvConverter kvConverter, BaseOptionsConf outputOptionsConf, BaseOptionsConf inputOptionsConf) {
        this.kvConverter = kvConverter;
        this.outputOptionsConf = outputOptionsConf;
        this.inputOptionsConf = inputOptionsConf;
    }

    public KvConverter getKvConverter(){
        return kvConverter;
    }

    public BaseOptionsConf getOutputOptionsConf(){
        return outputOptionsConf;
    }

    public BaseOptionsConf getInputOptionsConf(){
        return inputOptionsConf;
    }
}
