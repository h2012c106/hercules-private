package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;

public class MysqlCanalKvSerDerSupplier extends BaseKvSerDerSupplier {

    @Override
    public KvSerDer<?, ?, ?> getKvSerDer() {
        return new MysqlCanalEntryKvSerDer(options);
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
