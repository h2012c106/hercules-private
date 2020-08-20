package com.xiaohongshu.db.hercules.core.datasource;

public enum DataSourceRole {
    SOURCE,
    TARGET;

    public boolean isSource() {
        return SOURCE.equals(this);
    }

    public boolean isTarget() {
        return TARGET.equals(this);
    }
}