package com.xiaohongshu.db.hercules.common.parser;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;

public class CommonParser extends BaseParser {

    public CommonParser() {
        super(new CommonOptionsConf());
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return null;
    }

    @Override
    public DataSource getDataSource() {
        return null;
    }

}
