package com.xiaohongshu.db.hercules.core.datatype;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 由于允许用户指定column-type，从字符串到对应类型对象实际就是一个工厂行为。
 * {@link BaseDataType}因为是个enum，相当于同时拥有实例（枚举值）和工厂（类本身），且全局唯一，使用enum没毛病。
 * 而自定类型由于需要做接口抽象，并全局会被多地分别定义，但静态方法无继承一说，虽然如果硬要调用可以用反射但相当没必要，会极大弱化对数据源撰写的约束，
 * 所以必定需要一个可实例化的对象来做类型工厂。
 * 仅会在parse字符串类型的column type时用到。
 *
 * @param <I> 同CustomDataType，用于约束子类别把乱七八糟的CustomDataType塞进来，在外部使用本类时没啥用。
 * @param <O> 同CustomDataType，用于约束子类别把乱七八糟的CustomDataType塞进来，在外部使用本类时没啥用。
 */
public abstract class BaseCustomDataTypeManager<I, O> implements CustomDataTypeManager<I, O> {

    private static final Log LOG = LogFactory.getLog(BaseCustomDataTypeManager.class);

    private final Map<String, Class<? extends CustomDataType<I, O, ?>>> factory = new HashMap<>();

    public BaseCustomDataTypeManager() {
        for (Class<? extends CustomDataType<I, O, ?>> item : generateTypeList()) {
            CustomDataType<I, O, ?> itemInstance = construct(item);
            String typeName = itemInstance.getName();
            if (factory.containsKey(typeName)) {
                throw new RuntimeException("Duplicate definition of custom type: " + typeName);
            } else {
                factory.put(typeName, item);
            }
        }
    }

    private CustomDataType<I, O, ?> construct(Class<? extends CustomDataType<I, O, ?>> clazz) {
        CustomDataType<I, O, ?> itemInstance;
        try {
            Constructor<? extends CustomDataType<I, O, ?>> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            itemInstance = constructor.newInstance();
            constructor.setAccessible(false);
            return itemInstance;
        } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    abstract protected List<Class<? extends CustomDataType<I, O, ?>>> generateTypeList();

    private static class DataTypeInfo {
        private String dataTypeName;
        private List<String> params;

        public DataTypeInfo(String dataTypeName, List<String> params) {
            this.dataTypeName = dataTypeName;
            this.params = params;
        }

        public String getDataTypeName() {
            return dataTypeName;
        }

        public List<String> getParams() {
            return params;
        }
    }

    private DataTypeInfo parseDataType(String name) {
        String dataTypeName;
        List<String> params = Collections.EMPTY_LIST;
        // 解析类似Decimal(2,1)的定义
        // TODO 巨糙无比，先能用再说
        if (StringUtils.contains(name, '(')) {
            String regex = "(\\w+)\\((.+)\\)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) {
                dataTypeName = matcher.group(1);
                params = Arrays.stream(matcher.group(2).split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
            } else {
                dataTypeName = name;
            }
        } else {
            dataTypeName = name;
        }
        return new DataTypeInfo(dataTypeName, params);
    }

    @Override
    public boolean contains(String typeName) {
        return factory.containsKey(typeName);
    }

    @Override
    public CustomDataType<I, O, ?> get(String name) {
        DataTypeInfo dataTypeInfo = parseDataType(name);
        String dataTypeName = dataTypeInfo.getDataTypeName();
        List<String> params = dataTypeInfo.getParams();

        Class<? extends CustomDataType<I, O, ?>> customDataTypeClass = factory.get(dataTypeName);
        if (customDataTypeClass == null) {
            throw new RuntimeException("Cannot get special data type with name: " + name);
        }
        CustomDataType<I, O, ?> customDataType = construct(customDataTypeClass);
        customDataType.initialize(params);
        return customDataType;
    }

    @Override
    public CustomDataType<I, O, ?> getIgnoreCase(String name) {
        DataTypeInfo dataTypeInfo = parseDataType(name);
        String dataTypeName = dataTypeInfo.getDataTypeName();
        List<String> params = dataTypeInfo.getParams();

        for (Map.Entry<String, Class<? extends CustomDataType<I, O, ?>>> entry : factory.entrySet()) {
            if (StringUtils.equalsIgnoreCase(dataTypeName, entry.getKey())) {
                CustomDataType<I, O, ?> customDataType = construct(entry.getValue());
                customDataType.initialize(params);
                return customDataType;
            }
        }
        throw new RuntimeException("Cannot get special data type with case-ignored name: " + name);
    }
}
