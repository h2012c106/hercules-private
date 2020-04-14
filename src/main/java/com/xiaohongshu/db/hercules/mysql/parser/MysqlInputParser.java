package com.xiaohongshu.db.hercules.mysql.parser;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSInputParser;

public class MysqlInputParser extends RDBMSInputParser {

    @Override
    public DataSource getDataSource() {
        return DataSource.MySQL;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new MysqlInputOptionsConf();
    }
}
