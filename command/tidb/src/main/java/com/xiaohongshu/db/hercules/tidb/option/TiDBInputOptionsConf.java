package com.xiaohongshu.db.hercules.tidb.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.mysql.option.MysqlInputOptionsConf;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.FETCH_SIZE;

public final class TiDBInputOptionsConf extends BaseOptionsConf {

    public static final String SECONDARY_SPLIT_SIZE = "secondary-split-size";

    private static final long DEFAULT_SECONDARY_SPLIT_SIZE = 10000;

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(new MysqlInputOptionsConf());
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(SECONDARY_SPLIT_SIZE)
                .needArg(true)
                .description("The split size during map stage, which is designed for 'GC lifetime' exception.")
                .defaultStringValue(String.valueOf(DEFAULT_SECONDARY_SPLIT_SIZE))
                .build());
        return tmpList;
    }

    @Override
    protected List<String> deleteOptions() {
        // 低版本tidb不支持服务器侧游标，全部使用客户端侧游标
        return Lists.newArrayList(
                FETCH_SIZE
        );
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ParseUtils.assertTrue(options.getLong(SECONDARY_SPLIT_SIZE, null) > 0, "The split size must > 0.");
    }
}
