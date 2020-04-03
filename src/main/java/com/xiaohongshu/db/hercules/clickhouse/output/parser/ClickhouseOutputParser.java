package com.xiaohongshu.db.hercules.clickhouse.output.parser;

import com.xiaohongshu.db.hercules.clickhouse.output.options.ClickhouseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.parser.RDBMSOutputParser;

public class ClickhouseOutputParser extends RDBMSOutputParser {
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
