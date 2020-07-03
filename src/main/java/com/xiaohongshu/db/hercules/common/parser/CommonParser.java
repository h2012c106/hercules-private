package com.xiaohongshu.db.hercules.common.parser;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;

public class CommonParser extends BaseParser {

    public CommonParser() {
        super(new CommonOptionsConf(), null, (DataSourceRole) null);
    }

}
