package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;

public class MysqlCanalKvConverterSupplier implements KvConverterSupplier{

    @Override
    public KvConverter getKvConverter() {
        return new MysqlCanalEntryKvConverter();

    }

    @Override
    public BaseOptionsConf getOptionsConf() {
        return new CanalMysqlOptionConf();
    }
}
