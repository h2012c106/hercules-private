package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerializerSupplier;

public class MysqlCanalKvSerializerSupplier extends BaseKvSerializerSupplier {

    @Override
    public KvSerializer<?, ?, ?> getKvSerializer() {
        return new MysqlCanalEntryKvSerializer(options);
    }

    @Override
    public BaseOptionsConf getOutputOptionsConf() {
        return new CanalMysqlInputOptionConf();
    }

    @Override
    public BaseOptionsConf getInputOptionsConf() {
        return new CanalMysqlOutputOptionConf();
    }

}
