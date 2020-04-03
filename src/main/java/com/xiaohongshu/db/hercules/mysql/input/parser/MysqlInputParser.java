package com.xiaohongshu.db.hercules.mysql.input.parser;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.mysql.input.options.MysqlInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.input.parser.RDBMSInputParser;

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
