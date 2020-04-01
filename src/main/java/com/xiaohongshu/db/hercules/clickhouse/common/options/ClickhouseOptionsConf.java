package com.xiaohongshu.db.hercules.clickhouse.common.options;

import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;

import java.util.List;

public class ClickhouseOptionsConf extends RDBMSOptionsConf {

    private static final String DEFAULT_DRIVER_CLASS = "ru.yandex.clickhouse.ClickHouseDriver";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.add(SingleOptionConf.builder()
                .name(DRIVER)
                .needArg(true)
                .defaultStringValue(DEFAULT_DRIVER_CLASS)
                .description("The jdbc driver class name, e.g. 'com.mysql.jdbc.Driver'.")
                .build());
        return tmpList;
    }
}
