package com.xiaohongshu.db.hercules.clickhouse.input.parser;

import com.xiaohongshu.db.hercules.clickhouse.input.options.ClickhouseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.input.parser.RDBMSInputParser;

public class ClickhouseInputParser extends RDBMSInputParser {
    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new ClickhouseInputOptionsConf();
    }
}
