package com.xiaohongshu.db.hercules.converter.tidb;

import com.xiaohongshu.db.hercules.core.converter.KvConverter;
import com.xiaohongshu.db.hercules.converter.mysql.MysqlCanalKvConverterSupplier;

public class TidbCanalKvConverterSupplier extends MysqlCanalKvConverterSupplier {

    @Override
    public KvConverter<?, ?, ?> getKvConverter() {
        return new TidbCanalEntryKvConverter(options);
    }

}
