package com.xiaohongshu.db.hercules.elasticsearch;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class ElasticsearchDataSource implements DataSource {

    @Override
    public String name() {
        return "Elasticsearch";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
