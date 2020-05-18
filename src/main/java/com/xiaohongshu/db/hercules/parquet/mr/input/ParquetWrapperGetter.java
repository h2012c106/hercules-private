package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;

/**
 * 统一处理empty
 */
public abstract class ParquetWrapperGetter implements WrapperGetter<GroupWithSchemaInfo> {
    @Override
    public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
        if (row.isEmpty()) {
            return NullWrapper.INSTANCE;
        } else {
            return get(row, rowName, columnName);
        }
    }

    abstract protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception;
}
