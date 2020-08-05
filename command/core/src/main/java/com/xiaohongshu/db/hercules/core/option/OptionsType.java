package com.xiaohongshu.db.hercules.core.option;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;

public enum OptionsType {
    SOURCE("source", "source", DataSourceRole.SOURCE),
    TARGET("target", "target", DataSourceRole.TARGET),
    COMMON("", "common", null),
    SOURCE_CONVERTER("converter-source", "sconverter", null),
    TARGET_CONVERTER("converter-target", "tconverter", null);

    /**
     * 用户参数前缀
     */
    private String paramPrefix;
    /**
     * Configuration前缀
     */
    private String configSuffix;

    private DataSourceRole role;

    OptionsType(String paramPrefix, String configSuffix, DataSourceRole role) {
        this.paramPrefix = paramPrefix;
        this.configSuffix = configSuffix;
        this.role = role;
    }

    public String getParamPrefix() {
        return paramPrefix;
    }

    public String getConfigSuffix() {
        return configSuffix;
    }

    public DataSourceRole getRole() {
        return role;
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
