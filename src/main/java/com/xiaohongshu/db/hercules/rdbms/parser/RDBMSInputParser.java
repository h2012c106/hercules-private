package com.xiaohongshu.db.hercules.rdbms.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;

public class RDBMSInputParser extends BaseParser {

    public RDBMSInputParser() {
        this(new RDBMSInputOptionsConf(), DataSource.RDBMS);
    }

    public RDBMSInputParser(BaseOptionsConf optionsConf, DataSource dataSource) {
        super(optionsConf, dataSource, DataSourceRole.SOURCE);
    }

}
