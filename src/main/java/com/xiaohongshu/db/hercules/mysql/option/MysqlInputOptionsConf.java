package com.xiaohongshu.db.hercules.mysql.option;

import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;

import java.util.List;

public class MysqlInputOptionsConf extends RDBMSInputOptionsConf {

    public static final String DEFAULT_RANDOM_FUNC_NAME = "RAND()";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.addAll(new MysqlOptionsConf().getOptionsMap().values());
        tmpList.add(SingleOptionConf.builder()
                .name(RANDOM_FUNC_NAME)
                .needArg(true)
                .description("The random function used at balance mode sampling.")
                .build());
        return tmpList;
    }

}
