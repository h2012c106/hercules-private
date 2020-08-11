package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;

public class MongoOplogKvSerDerSupplier extends BaseKvSerDerSupplier {

    @Override
    public KvSerDer<?, ?, ?> getKvSerDer() {
        return new MongoOplogKvSerDer(options);
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
