package com.xiaohongshu.db.hercules.serder.canal.tidb;

import com.xiaohongshu.db.hercules.serder.canal.mysql.MysqlCanalKvSerDerSupplier;

public class TidbCanalKvSerDerSupplier extends MysqlCanalKvSerDerSupplier {
    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVSer<?> innerGetKVSer() {
        return new TiDBCanalMysqlEntryKVSer();
    }
}
