package com.xiaohongshu.db.hercules.converter.tidb;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.converter.mysql.MysqlCanalKvSerializerSupplier;

public class TidbCanalKvSerializerSupplier extends MysqlCanalKvSerializerSupplier {

    @Override
    public KvSerializer<?, ?, ?> getKvSerializer() {
        return new TidbCanalEntryKvSerializer(options);
    }

}
