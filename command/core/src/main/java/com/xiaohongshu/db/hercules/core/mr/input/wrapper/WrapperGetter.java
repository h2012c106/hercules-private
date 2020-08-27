package com.xiaohongshu.db.hercules.core.mr.input.wrapper;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;

/**
 * @param <T> 各种数据源读入时代表一行的数据结构
 */
public abstract class WrapperGetter<T> {

    abstract protected DataType getType();

    abstract protected boolean isNull(T row, String rowName, String columnName, int columnSeq) throws Exception;

    abstract protected BaseWrapper<?> getNonnull(T row, String rowName, String columnName, int columnSeq) throws Exception;

    /**
     * 从一行中拿出某一列的方法
     *
     * @param row
     * @param rowName    当前row的完整列名，主要用于Map结构
     * @param columnName
     * @param columnSeq
     * @return
     */
    public BaseWrapper<?> get(T row, String rowName, String columnName, int columnSeq) throws Exception {
        if (isNull(row, rowName, columnName, columnSeq)) {
            return NullWrapper.get(getType());
        } else {
            return getNonnull(row, rowName, columnName, columnSeq);
        }
    }
}
