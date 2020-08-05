package com.xiaohongshu.db.hercules.core.mr.output.wrapper;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import lombok.NonNull;

/**
 * @param <T> 各种数据源写出时代表一行的数据结构
 */
public abstract class WrapperSetter<T> {

    abstract protected void setNull(T row, String rowName, String columnName, int columnSeq) throws Exception;

    abstract protected void setNonnull(@NonNull BaseWrapper<?> wrapper, T row, String rowName, String columnName, int columnSeq) throws Exception;

    /**
     * 在{@param row}就地更改一列内容，无返回值
     *
     * @param wrapper
     * @param row
     * @param rowName    当前row的完整列名，主要用于Map结构
     * @param columnName
     * @param columnSeq
     */
    public void set(@NonNull BaseWrapper<?> wrapper, T row, String rowName, String columnName, int columnSeq) throws Exception {
        if (wrapper.isNull()) {
            setNull(row, rowName, columnName, columnSeq);
        } else {
            setNonnull(wrapper, row, rowName, columnName, columnSeq);
        }
    }
}
