package com.xiaohongshu.db.hercules.hbase.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;

public class HBaseOutputParser extends BaseParser {

//    public HBaseOutputParser(BaseOptionsConf optionsConf) {
//        super(optionsConf);
//    }

    public HBaseOutputParser() {
        super(new HBaseOutputOptionsConf());
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.TARGET;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.HBase;
    }
}
