package com.xiaohongshu.db.hercules.clickhouse.parser;

import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSInputParser;

public class ClickhouseInputParser extends RDBMSInputParser {
    public ClickhouseInputParser() {
        super(new ClickhouseInputOptionsConf(), DataSource.Clickhouse);
    }
}
