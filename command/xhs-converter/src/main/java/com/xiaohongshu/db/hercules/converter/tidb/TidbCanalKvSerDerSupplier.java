package com.xiaohongshu.db.hercules.converter.tidb;

import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.converter.mysql.MysqlCanalKvSerDerSupplier;

public class TidbCanalKvSerDerSupplier extends MysqlCanalKvSerDerSupplier {

    @Override
    public KvSerDer<?, ?, ?> getKvSerDer() {
        return new TidbCanalEntryKvSerDer(options);
    }

}
