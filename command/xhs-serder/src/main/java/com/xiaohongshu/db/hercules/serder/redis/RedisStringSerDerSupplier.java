package com.xiaohongshu.db.hercules.serder.redis;

import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;
import com.xiaohongshu.db.hercules.mongodb.schema.MongoDBDataTypeConverter;
import com.xiaohongshu.db.hercules.redis.schema.RedisDataTypeConverter;
import com.xiaohongshu.db.hercules.serder.mongo.MongoOplogKVSer;
import com.xiaohongshu.db.hercules.serder.mongo.MongoOplogOutputOptionConf;

/**
 * Created by jamesqq on 2020/11/3.
 */
public class RedisStringSerDerSupplier extends BaseKvSerDerSupplier {

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVSer<?> innerGetKVSer() {
        return new RedisStringKVSer();
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
        return new RedisStringKVOutputOptionConf();
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new RedisDataTypeConverter();
    }
}

