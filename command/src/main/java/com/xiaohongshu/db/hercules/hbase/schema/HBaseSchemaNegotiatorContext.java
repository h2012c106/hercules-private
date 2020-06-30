package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;

import java.util.List;
import java.util.Map;

public class HBaseSchemaNegotiatorContext extends BaseSchemaNegotiatorContext {

    public HBaseSchemaNegotiatorContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void afterAll(List<String> columnName, Map<String, DataType> columnType) {
        String rowKeyCol = getOptions().getString(HBaseOptionsConf.ROW_KEY_COL_NAME, null);
        if (rowKeyCol != null) {
            if (!columnName.contains(rowKeyCol)) {
                throw new RuntimeException("Missing row key col in column name list: " + columnName);
            }
        }
    }
}
