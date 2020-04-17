package com.xiaohongshu.db.hercules.core.serialize.datatype;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 没有对应的asXXX方法，因为本质上是一个wrapper的集合对象。
 * Map的value就是BaseWrapper，不要写成BaseWrapper的泛型实现形式，毕竟有可能数据源在一个Map value set里塞上不同的数据类型。
 * Map的key为键值可以理解为列名，String即可。
 * <p>
 * 同时作为{@link com.xiaohongshu.db.hercules.core.serialize.HerculesWritable}的内部存储类
 */
public class MapWrapper extends BaseWrapper<Map<String, BaseWrapper>> {

    private final static DataType DATA_TYPE = DataType.MAP;
    private final static String UNSUPPORTED_MESSAGE = "Unsupported to convert map wrapper to any basic data type, except string.";

    public MapWrapper() {
        this(1);
    }

    public MapWrapper(int size) {
        super(new HashMap<>((int) ((float) size / 0.75F + 1.0F)),
                DATA_TYPE,
                0);
    }

    private void oneLevelPut(String columnName, @NonNull BaseWrapper value) {
        getValue().put(columnName, value);
        addByteSize(value.getByteSize());
    }

    /**
     * 允许'xxx.yyy.zz'这样传列名，代表嵌套结构
     *
     * @param columnNameListStr
     * @param value
     */
    private void multiLevelPut(String columnNameListStr, @NonNull BaseWrapper value) {
        List<String> columnNameList
                = Arrays.asList(columnNameListStr.split(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX));
        MapWrapper tmpMapWrapper = this;
        for (int i = 0; i < columnNameList.size() - 1; ++i) {
            // 当前map加byte size，最后一个map不用加，one level里会加
            tmpMapWrapper.addByteSize(value.getByteSize());

            String columnName = columnNameList.get(i);
            BaseWrapper tmpWrapper = tmpMapWrapper.oneLevelGet(columnName);
            if (tmpWrapper == null) {
                tmpWrapper = new MapWrapper();
                tmpMapWrapper.oneLevelPut(columnName, tmpWrapper);
            }
            // 如果不是Map类型的直接报错
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        tmpMapWrapper.oneLevelPut(columnNameList.get(columnNameList.size() - 1), value);
    }

    public void put(String str, @NonNull BaseWrapper value, boolean oneLevel) {
        if (oneLevel) {
            oneLevelPut(str, value);
        } else {
            multiLevelPut(str, value);
        }
    }

    private BaseWrapper oneLevelGet(String columnName) {
        return getValue().get(columnName);
    }

    /**
     * 允许'xxx.yyy.zz'这样传列名，代表嵌套结构
     *
     * @param columnNameListStr
     * @return 有就返回，在任意一层没有就null
     */
    private BaseWrapper multiLevelGet(String columnNameListStr) {
        List<String> columnNameList
                = Arrays.asList(columnNameListStr.split(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX));
        MapWrapper tmpMapWrapper = this;
        for (int i = 0; i < columnNameList.size() - 1; ++i) {
            String columnName = columnNameList.get(i);
            BaseWrapper tmpWrapper = tmpMapWrapper.oneLevelGet(columnName);
            // 如果不包含这列或者这列不是Map，说明不存在
            if (!(tmpWrapper instanceof MapWrapper)) {
                return null;
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        return tmpMapWrapper.oneLevelGet(columnNameList.get(columnNameList.size() - 1));
    }

    public BaseWrapper get(String str, boolean oneLevel) {
        if (oneLevel) {
            return oneLevelGet(str);
        } else {
            return multiLevelGet(str);
        }
    }

    private boolean oneLevelContainsColumn(String columnName) {
        return getValue().containsKey(columnName);
    }

    private boolean multiLevelContainsColumn(String columnNameListStr) {
        List<String> columnNameList
                = Arrays.asList(columnNameListStr.split(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX));
        MapWrapper tmpMapWrapper = this;
        for (int i = 0; i < columnNameList.size() - 1; ++i) {
            String columnName = columnNameList.get(i);
            BaseWrapper tmpWrapper = tmpMapWrapper.oneLevelGet(columnName);
            // 如果不包含这列或者这列不是Map，说明不存在
            if (!(tmpWrapper instanceof MapWrapper)) {
                return false;
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        return tmpMapWrapper.oneLevelContainsColumn(columnNameList.get(columnNameList.size() - 1));
    }

    public boolean containsColumn(String str, boolean oneLevel) {
        if (oneLevel) {
            return oneLevelContainsColumn(str);
        } else {
            return multiLevelContainsColumn(str);
        }
    }

    @Override
    public Long asLong() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Double asDouble() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public BigInteger asBigInteger() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Boolean asBoolean() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Date asDate() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public String asString() {
        return asJson().toJSONString();
    }

    @Override
    public byte[] asBytes() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public JSON asJson() {
        return JSONObject.parseObject(JSON.toJSONString(asDefault()));
    }

    @Override
    public Object asDefault() {
        return getValue().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().asDefault()));
    }
}
