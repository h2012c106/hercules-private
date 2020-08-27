package com.xiaohongshu.db.hercules.myhub;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class MyhubDataSource implements DataSource {
    @Override
    public String name() {
        return "Myhub";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
