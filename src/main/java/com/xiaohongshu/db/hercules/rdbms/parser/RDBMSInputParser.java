package com.xiaohongshu.db.hercules.rdbms.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;

public class RDBMSInputParser extends BaseParser {

    public RDBMSInputParser() {
        super(new RDBMSInputOptionsConf());
    }

    public RDBMSInputParser(BaseOptionsConf optionsConf) {
        super(optionsConf);
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.SOURCE;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.RDBMS;
    }

}
