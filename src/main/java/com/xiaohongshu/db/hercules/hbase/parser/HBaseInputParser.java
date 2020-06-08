package com.xiaohongshu.db.hercules.hbase.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;

public class HBaseInputParser extends BaseParser {

    public HBaseInputParser() {
        super(new HBaseInputOptionsConf(), DataSource.HBase, DataSourceRole.SOURCE);
    }

}
