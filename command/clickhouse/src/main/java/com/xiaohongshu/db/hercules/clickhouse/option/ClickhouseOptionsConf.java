package com.xiaohongshu.db.hercules.clickhouse.option;

import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf.DRIVER;

public final class ClickhouseOptionsConf extends BaseOptionsConf {

    private static final String DEFAULT_DRIVER_CLASS = "ru.yandex.clickhouse.ClickHouseDriver";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(DRIVER)
                .needArg(true)
                .defaultStringValue(DEFAULT_DRIVER_CLASS)
                .description("The jdbc driver class name, e.g. 'com.mysql.jdbc.Driver'.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
