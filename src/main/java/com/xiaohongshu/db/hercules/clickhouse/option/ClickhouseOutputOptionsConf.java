package com.xiaohongshu.db.hercules.clickhouse.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf.*;

public final class ClickhouseOutputOptionsConf extends BaseOptionsConf {

    public static final String ENABLE_NULL = "enable-null";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new RDBMSOutputOptionsConf(),
                new ClickhouseOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(ENABLE_NULL)
                .needArg(false)
                .description("If specified, will not use the default value to represent null.")
                .build());
        return res;
    }

    @Override
    protected List<String> deleteOptions() {
        // clickhouse jdbc完全没有commit行为
        return Lists.newArrayList(AUTOCOMMIT, STATEMENT_PER_COMMIT);
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ParseUtils.assertTrue(ExportType
                .valueOfIgnoreCase(options.getString(EXPORT_TYPE, null))
                .isInsert(), "Clickhouse only support INSERT export type.");
    }
}
