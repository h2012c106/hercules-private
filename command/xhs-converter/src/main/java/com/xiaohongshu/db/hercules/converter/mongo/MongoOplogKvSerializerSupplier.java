package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerializerSupplier;

public class MongoOplogKvSerializerSupplier extends BaseKvSerializerSupplier {

    @Override
    public KvSerializer<?, ?, ?> getKvSerializer() {
        return new MongoOplogKvSerializer(options);
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
