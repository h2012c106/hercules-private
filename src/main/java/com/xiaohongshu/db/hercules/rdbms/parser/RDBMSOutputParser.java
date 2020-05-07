package com.xiaohongshu.db.hercules.rdbms.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;

public class RDBMSOutputParser extends BaseParser {

    public RDBMSOutputParser() {
        super(new RDBMSOutputOptionsConf());
    }

    public RDBMSOutputParser(BaseOptionsConf optionsConf) {
        super(optionsConf);
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.TARGET;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.RDBMS;
    }
}
