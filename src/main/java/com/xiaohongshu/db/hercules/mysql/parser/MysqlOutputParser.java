package com.xiaohongshu.db.hercules.mysql.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSOutputParser;

public class MysqlOutputParser extends RDBMSOutputParser {

    public MysqlOutputParser() {
        super(new MysqlOutputOptionsConf(), DataSource.MySQL);
    }

}