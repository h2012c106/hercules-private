package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;

/**
 * @param <T> 各种数据源读入时代表一行的数据结构
 */
public interface WrapperGetter<T> {
    /**
     * 从一行中拿出某一列的方法
     *
     * @param row
     * @param name
     * @param seq
     * @return
     */
    public BaseWrapper get(T row, String name, int seq) throws Exception;
}
