package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

/**
 * @param <T> 各种数据源读入时代表一行的数据结构
 */
public interface WrapperGetter<T> {
    /**
     * 从一行中拿出某一列的方法
     *
     * @param row
     * @param rowName 当前row的完整列名，主要用于Map结构
     * @param columnName
     * @param columnSeq
     * @return
     */
    public BaseWrapper get(T row, String rowName, String columnName, int columnSeq) throws Exception;
}
