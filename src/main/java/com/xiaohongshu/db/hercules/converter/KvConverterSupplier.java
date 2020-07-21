package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public abstract class KvConverterSupplier<T, K, L> {

    private final KvConverter<T, K, L> kvConverter;
    private final BaseOptionsConf outputOptionsConf;
    private final BaseOptionsConf inputOptionsConf;
    private final WrapperSetterFactory wrapperSetterFactory;
    private final WrapperGetterFactory wrapperGetterFactory;

    public KvConverterSupplier(KvConverter<T, K, L> kvConverter, BaseOptionsConf outputOptionsConf, BaseOptionsConf inputOptionsConf, WrapperSetterFactory wrapperSetterFactory, WrapperGetterFactory wrapperGetterFactory) {
        this.kvConverter = kvConverter;
        this.outputOptionsConf = outputOptionsConf;
        this.inputOptionsConf = inputOptionsConf;
        this.wrapperSetterFactory = wrapperSetterFactory;
        this.wrapperGetterFactory = wrapperGetterFactory;
    }

    public KvConverter<T, K, L> getKvConverter(){
        return kvConverter;
    }

    public BaseOptionsConf getOutputOptionsConf(){
        return outputOptionsConf;
    }

    public BaseOptionsConf getInputOptionsConf(){
        return inputOptionsConf;
    }

    public WrapperSetterFactory getWrapperSetterFactory() {
        return wrapperSetterFactory;
    }

    public WrapperGetterFactory getWrapperGetterFactory() {
        return wrapperGetterFactory;
    }
}
