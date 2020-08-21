package com.xiaohongshu.db.hercules.redis;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;


public class RedisDataSource implements DataSource {

    @Override
    public String name() {
        return "Redis";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }


}
