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

    @Override
    public DataSource getDataSource() {
        return DataSource.Clickhouse;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new ClickhouseOutputOptionsConf();
    }

    @Override
    protected void validateOptions(GenericOptions options) {
        super.validateOptions(options);

        ParseUtils.assertTrue(ExportType
                .valueOfIgnoreCase(options.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null))
                .isInsert(), "Clickhouse only support INSERT export type.");
    }
}
