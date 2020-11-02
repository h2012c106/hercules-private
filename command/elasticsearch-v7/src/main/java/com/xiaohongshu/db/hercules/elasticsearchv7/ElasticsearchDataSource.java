package com.xiaohongshu.db.hercules.elasticsearchv7;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class ElasticsearchDataSource implements DataSource {

    @Override
    public String name() {
        return "ElasticsearchV7";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
