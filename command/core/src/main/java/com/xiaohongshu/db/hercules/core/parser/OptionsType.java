package com.xiaohongshu.db.hercules.core.parser;

public enum OptionsType {
    SOURCE,
    TARGET,
    COMMON;

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
