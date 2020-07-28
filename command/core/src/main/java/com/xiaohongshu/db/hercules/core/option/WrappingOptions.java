package com.xiaohongshu.db.hercules.core.option;

import org.apache.hadoop.conf.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

public class WrappingOptions {
    private final Map<OptionsType, GenericOptions> genericOptionsMap;

    public WrappingOptions() {
        genericOptionsMap = new LinkedHashMap<>();
    }

    public WrappingOptions(GenericOptions... optionsArray) {
        genericOptionsMap = new LinkedHashMap<>();
        for (GenericOptions options : optionsArray) {
            add(options);
        }
    }

    public void add(GenericOptions options) {
        genericOptionsMap.put(options.getOptionsType(), options);
    }

    public GenericOptions getGenericOptions(OptionsType optionsType) {
        return genericOptionsMap.getOrDefault(optionsType, new GenericOptions(optionsType));
    }

    public GenericOptions getCommonOptions() {
        return getGenericOptions(OptionsType.COMMON);
    }

    public GenericOptions getSourceOptions() {
        return getGenericOptions(OptionsType.SOURCE);
    }

    public GenericOptions getTargetOptions() {
        return getGenericOptions(OptionsType.TARGET);
    }

    public void toConfiguration(Configuration configuration) {
        for (GenericOptions options : genericOptionsMap.values()) {
            options.toConfiguration(configuration);
        }
    }

    public void fromConfiguration(Configuration configuration) {
        for (OptionsType optionsType : OptionsType.values()) {
            genericOptionsMap.put(optionsType, GenericOptions.fromConfiguration(configuration, optionsType));
        }
    }

    @Override
    public String toString() {
        return genericOptionsMap.toString();
    }
}
