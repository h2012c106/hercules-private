package com.xiaohongshu.db.hercules.elasticsearchv6;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class ElasticsearchDataSource implements DataSource {

    @Override
    public String name() {
        return "ElasticsearchV6";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
