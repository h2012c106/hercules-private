package com.xiaohongshu.db.hercules.hbase.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;

public class HBaseInputParser extends BaseParser {

//    public HBaseInputParser(BaseOptionsConf optionsConf) {
//        super(optionsConf);
//    }

    public HBaseInputParser() {
        super(new HBaseInputOptionsConf());
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.SOURCE;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.HBase;
    }
}
