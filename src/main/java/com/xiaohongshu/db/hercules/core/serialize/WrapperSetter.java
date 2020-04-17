package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import lombok.NonNull;

/**
 * @param <T>各种数据源写出时代表一行的数据结构
 */
public interface WrapperSetter<T> {
    /**
     * 在{@param row}就地更改一列内容，无返回值
     *
     * @param wrapper
     * @param row
     * @param name
     * @param seq
     */
    public void set(@NonNull BaseWrapper wrapper, T row, String name, int seq) throws Exception;
}
