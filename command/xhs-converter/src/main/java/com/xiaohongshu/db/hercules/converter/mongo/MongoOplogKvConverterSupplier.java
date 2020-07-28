package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.core.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvConverterSupplier;

public class MongoOplogKvConverterSupplier extends BaseKvConverterSupplier {

    @Override
    public KvConverter<?, ?, ?> getKvConverter() {
        return new MongoOplogKvConverter(options);
    }

    @Override
    public BaseOptionsConf getOutputOptionsConf() {
        return new MongoOplogOutputOptionConf();
    }

    @Override
    public BaseOptionsConf getInputOptionsConf() {
        return new MongoOplogInputOptionConf();
    }

}
