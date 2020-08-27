package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSFastSplitterGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.input.SplitGetter;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;

public class ClickhouseInputFormat extends RDBMSInputFormat {

    @Override
    protected SplitGetter getSplitGetter(GenericOptions options) {
        if (options.getBoolean(RDBMSInputOptionsConf.BALANCE_SPLIT, true)) {
            return new ClickhouseBalanceSplitGetter();
        } else {
            return new RDBMSFastSplitterGetter();
        }
    }

}
