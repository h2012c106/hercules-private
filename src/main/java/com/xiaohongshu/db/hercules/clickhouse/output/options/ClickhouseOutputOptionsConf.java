package com.xiaohongshu.db.hercules.clickhouse.output.options;

import com.xiaohongshu.db.hercules.clickhouse.common.options.ClickhouseOptionsConf;
import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;

import java.util.List;

public class ClickhouseOutputOptionsConf extends RDBMSOutputOptionsConf {
    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.addAll(new ClickhouseOptionsConf().getOptionsMap().values());
        // clickhouse jdbc完全没有commit行为
        clearOption(tmpList, AUTOCOMMIT);
        clearOption(tmpList, STATEMENT_PER_COMMIT);
        return tmpList;
    }
}
