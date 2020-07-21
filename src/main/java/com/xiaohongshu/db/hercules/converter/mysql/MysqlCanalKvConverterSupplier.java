package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public class MysqlCanalKvConverterSupplier extends KvConverterSupplier<Integer, CanalEntry.Column, CanalEntry.Column.Builder>  {

    public MysqlCanalKvConverterSupplier(GenericOptions options) {
        super(new MysqlCanalEntryKvConverter(options), new CanalMysqlOutputOptionConf(), new CanalMysqlInputOptionConf(), new CanalMysqlWrapperSetterFactory(), new CanalMysqlWrapperGetterFactory());
    }
}
