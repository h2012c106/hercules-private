package com.xiaohongshu.db.hercules.core.option;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.math.BigDecimal;
import java.util.*;

public final class GenericOptions {

    private static final String PREFIX = "hercules";
    private static final String CONFIGURATION_DELIMITER = ".";

    private final OptionsType optionsType;
    private final HashMap<String, String> properties;

    private static final String ARRAY_DELIMITER = "@%HHXHH%@";
    /**
     * 空值不会被传到map，需要一个有长度的字符串来表明这是空字符串
     */
    private static final String EMPTY_PLACEHOLDER = "@%HHX_HERCULES_EMPTY_PLACEHOLDER_XHH%@";

    public GenericOptions(OptionsType optionsType) {
        this.optionsType = optionsType;
        this.properties = new LinkedHashMap<>();
    }

    public OptionsType getOptionsType() {
        return optionsType;
    }

    public void set(String key, Object value) {
        properties.put(key, value.toString());
    }

    public void set(String key, String[] value) {
        properties.put(key, String.join(ARRAY_DELIMITER, value));
    }

    public boolean hasProperty(String key) {
        return key != null && properties.containsKey(key);
    }

    private <T> T innerGet(String key, T defaultValue, StringConverter<T> converter) {
        String resStr = properties.get(key);
        if (resStr != null) {
            return converter.convert(resStr);
        } else {
            return defaultValue;
        }
    }

    public String getString(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return innerGet(key, defaultValue, new StringConverter<Integer>() {
            @Override
            public Integer convert(String sourceString) {
                return Integer.parseInt(sourceString);
            }
        });
    }

    public Long getLong(String key, Long defaultValue) {
        return innerGet(key, defaultValue, new StringConverter<Long>() {
            @Override
            public Long convert(String sourceString) {
                return Long.parseLong(sourceString);
            }
        });
    }

    public Double getDouble(String key, Double defaultValue) {
        return innerGet(key, defaultValue, new StringConverter<Double>() {
            @Override
            public Double convert(String sourceString) {
                return Double.parseDouble(sourceString);
            }
        });
    }

    public BigDecimal getDecimal(String key, BigDecimal defaultValue) {
        return innerGet(key, defaultValue, new StringConverter<BigDecimal>() {
            @Override
            public BigDecimal convert(String sourceString) {
                return new BigDecimal(sourceString);
            }
        });
    }

    public Boolean getBoolean(String key, boolean defaultValue) {
        return innerGet(key, defaultValue, new StringConverter<Boolean>() {
            @Override
            public Boolean convert(String sourceString) {
                return Boolean.parseBoolean(sourceString);
            }
        });
    }

    public String[] getStringArray(String key, String[] defaultValue, final boolean trim, final boolean filterEmpty) {
        return innerGet(key, defaultValue, new StringConverter<String[]>() {
            @Override
            public String[] convert(String sourceString) {
                return Arrays.stream(sourceString.split(ARRAY_DELIMITER))
                        .map(item -> trim ? item.trim() : item)
                        .filter(item -> !filterEmpty || item.length() > 0)
                        .toArray(String[]::new);
            }
        });
    }

    public String[] getTrimmedStringArray(String key, String[] defaultValue) {
        return getStringArray(key, defaultValue, true, true);
    }

    public JSONObject getJson(String key, JSONObject defaultValue) {
        return innerGet(key, defaultValue, new StringConverter<JSONObject>() {
            @Override
            public JSONObject convert(String sourceString) {
                return JSON.parseObject(sourceString);
            }
        });
    }

    public static String getConfigurationName(String param, OptionsType type) {
        return String.join(CONFIGURATION_DELIMITER, Lists.newArrayList(PREFIX, param, type.getConfigSuffix()));
    }

    public void toConfiguration(Configuration configuration) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            key = String.join(CONFIGURATION_DELIMITER, Lists.newArrayList(PREFIX, key, optionsType.getConfigSuffix()));
            configuration.set(key, value.length() == 0 ? EMPTY_PLACEHOLDER : value);
        }
    }

    public static GenericOptions fromConfiguration(Configuration configuration, OptionsType optionsType) {
        GenericOptions res = new GenericOptions(optionsType);
        String suffix = optionsType.getConfigSuffix();
        String prefix = PREFIX + CONFIGURATION_DELIMITER;
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (!StringUtils.startsWith(key, prefix)) {
                continue;
            } else {
                key = key.substring(prefix.length());
            }
            String value = entry.getValue();
            if (key.endsWith(suffix)) {
                key = key.substring(0, key.length() - (CONFIGURATION_DELIMITER + suffix).length());
                res.set(key, EMPTY_PLACEHOLDER.equals(value) ? "" : value);
            }
        }
        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericOptions that = (GenericOptions) o;
        return Objects.equal(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(properties);
    }

    interface StringConverter<T> {
        /**
         * @param sourceString 一定是非null string，由{@link #innerGet}逻辑保证
         * @return
         */
        T convert(String sourceString);
    }

    @Override
    public String toString() {
        return properties.toString();
    }
}
