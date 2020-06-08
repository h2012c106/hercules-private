package com.xiaohongshu.db.hercules.mysql.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSInputParser;

public class MysqlInputParser extends RDBMSInputParser {

    public MysqlInputParser() {
        this(new MysqlInputOptionsConf(), DataSource.MySQL);
    }

    public MysqlInputParser(BaseOptionsConf optionsConf, DataSource dataSource) {
        super(optionsConf, dataSource);
    }
}
