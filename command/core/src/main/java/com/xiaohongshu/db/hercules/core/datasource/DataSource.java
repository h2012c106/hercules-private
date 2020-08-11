package com.xiaohongshu.db.hercules.core.datasource;

public interface DataSource {

    public String name();

    public String getFilePositionParam();

    default public boolean hasKvSerDer() {
        return false;
    }

}
