package com.xiaohongshu.db.hercules.mysql.output.options;

import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;
import com.xiaohongshu.db.hercules.mysql.common.options.MysqlOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;

import java.util.List;

public class MysqlOutputOptionsConf extends RDBMSOutputOptionsConf {

    public static final String ALLOW_ZERO_DATE = "allow-zero-date";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.addAll(new MysqlOptionsConf().getOptionsMap().values());
        tmpList.add(SingleOptionConf.builder()
                .name(ALLOW_ZERO_DATE)
                .needArg(false)
                .description("Whether to allow insert '0000-00-00 00:00:00' as timestamp.")
                .build());
        return tmpList;
    }
}
