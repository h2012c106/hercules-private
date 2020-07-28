package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.core.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvConverterSupplier;

public class MysqlCanalKvConverterSupplier extends BaseKvConverterSupplier {

    @Override
    public KvConverter<?, ?, ?> getKvConverter() {
        return new MysqlCanalEntryKvConverter(options);
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
