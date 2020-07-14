package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;

import java.sql.ResultSet;

public class TidbCanalKvConverterSupplier  extends KvConverterSupplier<Integer, ResultSet, CanalEntry.Column, CanalEntry.Column.Builder> {

    public TidbCanalKvConverterSupplier() {
        super(new TidbCanalEntryKvConverter(), new CanalMysqlOutputOptionConf(), new CanalMysqlInputOptionConf());
    }
}
