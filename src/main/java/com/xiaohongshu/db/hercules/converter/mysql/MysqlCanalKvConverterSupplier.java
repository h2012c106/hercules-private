package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public class MysqlCanalKvConverterSupplier extends KvConverterSupplier {

    public MysqlCanalKvConverterSupplier() {
        super(new MysqlCanalEntryKvConverter(), new CanalMysqlOptionConf());
    }
}
