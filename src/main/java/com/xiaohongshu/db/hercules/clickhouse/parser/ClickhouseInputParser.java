package com.xiaohongshu.db.hercules.clickhouse.parser;

import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSInputParser;

public class ClickhouseInputParser extends RDBMSInputParser {

    @Override
    public DataSource getDataSource() {
        return DataSource.Clickhouse;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new ClickhouseInputOptionsConf();
    }
}
