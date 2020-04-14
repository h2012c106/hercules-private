package com.xiaohongshu.db.hercules.core.option;

import lombok.Builder;
import lombok.NonNull;

/**
 * 用于配置单个配置项的类
 */
@Builder
public final class SingleOptionConf {
    @NonNull
    private String name;
    /**
     * 如果不需要arg，说明是根据有没有此参来判断01的布尔值，那么{@link #defaultStringValue}没必要填，填了也没用
     */
    @NonNull
    private boolean needArg;
    /**
     * 若是必须的参数，则{@link #defaultStringValue}没必要填，填了也没用，本配置晚于{@link #needArg}判断
     */
    @Builder.Default
    private boolean necessary = false;
    private String defaultStringValue;
    @NonNull
    private String description;
    @Builder.Default
    private boolean list = false;
    @Builder.Default
    private String listDelimiter = ",";

    public String getName() {
        return name;
    }

    public boolean isNeedArg() {
        return needArg;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultStringValue() {
        return defaultStringValue;
    }

    public boolean isList() {
        return list;
    }

    public String getListDelimiter() {
        return listDelimiter;
    }

    public boolean isNecessary() {
        return necessary;
    }
}
