package com.xiaohongshu.db.hercules.core.option;

public enum OptionsType {
    SOURCE("source"),
    TARGET("target"),
    COMMON("", "common"),
    SOURCE_CONVERTER("converter-source","sconverter"),
    TARGET_CONVERTER("converter-target","tconverter");

    /**
     * 用户参数前缀
     */
    private String paramPrefix;
    /**
     * Configuration前缀
     */
    private String configSuffix;

    OptionsType(String paramPrefix, String configSuffix) {
        this.paramPrefix = paramPrefix;
        this.configSuffix = configSuffix;
    }

    OptionsType(String fix) {
        this.paramPrefix = fix;
        this.configSuffix = fix;
    }

    public String getParamPrefix() {
        return paramPrefix;
    }

    public String getConfigSuffix() {
        return configSuffix;
    }

    public boolean isSource() {
        return SOURCE.equals(this);
    }

    public boolean isTarget() {
        return TARGET.equals(this);
    }

    public boolean isCommon() {
        return COMMON.equals(this);
    }
}
