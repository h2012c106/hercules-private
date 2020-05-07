package com.xiaohongshu.db.hercules.mysql.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSOutputParser;

public class MysqlOutputParser extends RDBMSOutputParser {

    public MysqlOutputParser() {
        super(new MysqlOutputOptionsConf());
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.MySQL;
    }
}
