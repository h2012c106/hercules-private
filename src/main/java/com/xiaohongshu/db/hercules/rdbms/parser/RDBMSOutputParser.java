package com.xiaohongshu.db.hercules.rdbms.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;

public class RDBMSOutputParser extends BaseParser {

    public RDBMSOutputParser() {
        this(new RDBMSOutputOptionsConf(), DataSource.RDBMS);
    }

    public RDBMSOutputParser(BaseOptionsConf optionsConf, DataSource dataSource) {
        super(optionsConf, dataSource, DataSourceRole.TARGET);
    }

}
