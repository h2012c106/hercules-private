package com.xiaohongshu.db.hercules.serder.mongo;

import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBDataTypeConverter;

public class MongoOplogKvSerDerSupplier extends BaseKvSerDerSupplier {

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVSer<?> innerGetKVSer() {
        return new MongoOplogKVSer();
    }

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVDer<?> innerGetKVDer() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new MongoOplogOutputOptionConf();
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new MongoDBDataTypeConverter();
    }
}
