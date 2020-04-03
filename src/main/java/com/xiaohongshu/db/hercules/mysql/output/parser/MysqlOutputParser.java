package com.xiaohongshu.db.hercules.mysql.output.parser;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.mysql.output.options.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.parser.RDBMSOutputParser;

public class MysqlOutputParser extends RDBMSOutputParser {

    @Override
    public DataSource getDataSource() {
        return DataSource.MySQL;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new MysqlOutputOptionsConf();
    }
}
