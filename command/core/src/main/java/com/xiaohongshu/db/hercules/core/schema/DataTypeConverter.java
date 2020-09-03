package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;

import java.util.Map;

/**
 * 这东西在{@link BaseSchemaFetcher}与读入的时候逐行判断时有用
 *
 * @param <T> 判断数据源类型的标准，例如sql为int，内部使用switch...case；mongo为Object，内部使用if...instance of
 * @param <L> 数据源读入时表示一行的数据结构
 * @author huanghanxiang
 */
public interface DataTypeConverter<T, L> {

    /**
     * 对单个数据元素判断内部类型
     *
     * @param standard
     * @return
     */
    DataType convertElementType(T standard);

    /**
     * 往回转
     *
     * @param type
     * @return
     */
    default T getElementType(DataType type) {
        throw new UnsupportedOperationException();
    }

    /**
     * 对一个数据行判断数据类型
     *
     * @param line
     * @return
     */
    Map<String, DataType> convertRowType(L line);
}
