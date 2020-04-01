package com.xiaohongshu.db.hercules.clickhouse.input.options;

import com.xiaohongshu.db.hercules.clickhouse.common.options.ClickhouseOptionsConf;
import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;
import com.xiaohongshu.db.hercules.mysql.common.options.MysqlOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;

import java.util.List;

public class ClickhouseInputOptionsConf extends RDBMSInputOptionsConf {

    private static final String DEFAULT_RANDOM_FUNC_NAME = "(RAND() / 4294967295)";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.addAll(new ClickhouseOptionsConf().getOptionsMap().values());
        tmpList.add(SingleOptionConf.builder()
                .name(RANDOM_FUNC_NAME)
                .needArg(true)
                .description("The random function used at balance mode sampling.")
                .build());
        return tmpList;
    }
}
