package com.xiaohongshu.db.hercules.clickhouse.parser;

import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSOutputParser;

public class ClickhouseOutputParser extends RDBMSOutputParser {

    public ClickhouseOutputParser() {
        super(new ClickhouseOutputOptionsConf(), DataSource.Clickhouse);
    }

}
