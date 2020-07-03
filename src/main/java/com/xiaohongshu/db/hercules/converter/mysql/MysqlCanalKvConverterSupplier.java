package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public class MysqlCanalKvConverterSupplier implements KvConverterSupplier {

    @Override
    public KvConverter getKvConverter() {
        return new MysqlCanalEntryKvConverter();

    }

    @Override
    public BaseOptionsConf getOptionsConf() {
        return new CanalMysqlOptionConf();
    }
}
