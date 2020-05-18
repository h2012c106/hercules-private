package com.xiaohongshu.db.hercules.tidb.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.mysql.parser.MysqlInputParser;
import com.xiaohongshu.db.hercules.tidb.option.TiDBInputOptionsConf;

public class TiDBInputParser extends MysqlInputParser {
    public TiDBInputParser() {
        super(new TiDBInputOptionsConf());
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.TiDB;
    }
}
