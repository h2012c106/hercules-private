package com.xiaohongshu.db.hercules.mongodb.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBInputOptionsConf;

public class MongoDBInputParser extends BaseParser {
    public MongoDBInputParser() {
        super(new MongoDBInputOptionsConf());
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.MongoDB;
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.SOURCE;
    }
}
