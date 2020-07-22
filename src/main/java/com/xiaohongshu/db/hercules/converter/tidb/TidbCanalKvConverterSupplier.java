package com.xiaohongshu.db.hercules.converter.tidb;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.converter.mysql.CanalMysqlInputOptionConf;
import com.xiaohongshu.db.hercules.converter.mysql.CanalMysqlOutputOptionConf;
import com.xiaohongshu.db.hercules.converter.mysql.CanalMysqlWrapperGetterFactory;
import com.xiaohongshu.db.hercules.converter.mysql.CanalMysqlWrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public class TidbCanalKvConverterSupplier  extends KvConverterSupplier<Integer, CanalEntry.Column, CanalEntry.Column.Builder> {

    public TidbCanalKvConverterSupplier(GenericOptions options) {
        super(new TidbCanalEntryKvConverter(options), new CanalMysqlOutputOptionConf(), new CanalMysqlInputOptionConf(), new CanalMysqlWrapperSetterFactory(), new CanalMysqlWrapperGetterFactory());
    }
}
