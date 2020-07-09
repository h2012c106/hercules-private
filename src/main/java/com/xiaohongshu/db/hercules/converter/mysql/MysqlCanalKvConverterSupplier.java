package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;

import java.sql.ResultSet;

public class MysqlCanalKvConverterSupplier extends KvConverterSupplier<Integer, ResultSet, CanalEntry.Column, CanalEntry.Column.Builder>  {

    public MysqlCanalKvConverterSupplier() {
        super(new MysqlCanalEntryKvConverter(), new CanalMysqlOutputOptionConf(), new CanalMysqlInputOptionConf());
    }
}
