package com.xiaohongshu.db.hercules.clickhouse.parser;

import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSOutputParser;

public class ClickhouseOutputParser extends RDBMSOutputParser {

    public ClickhouseOutputParser() {
        super(new ClickhouseOutputOptionsConf());
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.Clickhouse;
    }
}
