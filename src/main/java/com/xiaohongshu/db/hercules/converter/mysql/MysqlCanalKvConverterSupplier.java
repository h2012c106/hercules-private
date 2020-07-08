package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;

public class MysqlCanalKvConverterSupplier extends KvConverterSupplier {

    public MysqlCanalKvConverterSupplier() {
        super(new MysqlCanalEntryKvConverter(), new CanalMysqlOutputOptionConf(), new CanalMysqlInputOptionConf());
    }
}
