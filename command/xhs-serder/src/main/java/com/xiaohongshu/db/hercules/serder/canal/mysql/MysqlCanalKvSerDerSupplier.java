package com.xiaohongshu.db.hercules.serder.canal.mysql;

import com.xiaohongshu.db.hercules.mysql.schema.MysqlDataTypeConverter;
import com.xiaohongshu.db.hercules.serder.canal.CanalMysqlInputOptionConf;
import com.xiaohongshu.db.hercules.serder.canal.CanalMysqlOutputOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.OptionsConf;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.supplier.BaseKvSerDerSupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;

public class MysqlCanalKvSerDerSupplier extends BaseKvSerDerSupplier {

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVSer<?> innerGetKVSer() {
        return new MysqlCanalMysqlEntryKVSer();
    }

    @Override
    protected com.xiaohongshu.db.hercules.core.serder.KVDer<?> innerGetKVDer() {
        return new MysqlCanalMysqlEntryKVDer();
    }

    @Override
    protected OptionsConf innerGetInputOptionsConf() {
        return new CanalMysqlInputOptionConf();
    }

    @Override
    protected OptionsConf innerGetOutputOptionsConf() {
        return new CanalMysqlOutputOptionConf();
    }

    @Override
    protected DataTypeConverter<?, ?> innerGetDataTypeConverter() {
        return new MysqlDataTypeConverter();
    }

}
