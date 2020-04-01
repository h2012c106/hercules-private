package com.xiaohongshu.db.hercules.common.parser;

import com.xiaohongshu.db.hercules.common.options.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;

public class CommonParser extends BaseParser<CommonOptionsConf> {

    @Override
    public DataSource getDataSource() {
        return null;
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return null;
    }

    @Override
    protected CommonOptionsConf getOptionsConf() {
        return new CommonOptionsConf();
    }

    @Override
    protected void validateOptions(GenericOptions options) {
        Integer numMapper = options.getInteger(CommonOptionsConf.NUM_MAPPER, CommonOptionsConf.DEFAULT_NUM_MAPPER);
        ParseUtils.assertTrue(numMapper > 0, "Illegal num mapper: " + numMapper);

        // parse一下看看是不是json
        options.getJson(CommonOptionsConf.COLUMN_MAP, null);
    }
}
