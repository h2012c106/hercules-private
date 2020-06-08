package com.xiaohongshu.db.hercules.mongodb.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.mongodb.option.MongoDBOutputOptionsConf;

public class MongoDBOutputParser extends BaseParser {
    public MongoDBOutputParser() {
        super(new MongoDBOutputOptionsConf(), DataSource.MongoDB, DataSourceRole.TARGET);
    }
}
