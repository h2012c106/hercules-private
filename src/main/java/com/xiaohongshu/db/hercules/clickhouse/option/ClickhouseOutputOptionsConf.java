package com.xiaohongshu.db.hercules.clickhouse.option;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf.AUTOCOMMIT;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf.STATEMENT_PER_COMMIT;

public final class ClickhouseOutputOptionsConf extends BaseOptionsConf {
    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Collections.singletonList(new RDBMSOutputOptionsConf());
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        // clickhouse jdbc完全没有commit行为
        clearOption(tmpList, AUTOCOMMIT);
        clearOption(tmpList, STATEMENT_PER_COMMIT);
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
