package com.xiaohongshu.db.hercules.mysql.option;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf.EXPORT_TYPE;

public final class MysqlOutputOptionsConf extends BaseOptionsConf {

    public static final String ABANDON_ZERO_DATE = "allow-zero-date";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Collections.singletonList(new RDBMSOutputOptionsConf());
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(ABANDON_ZERO_DATE)
                .needArg(false)
                .description("Whether to abandon inserting '0000-00-00 00:00:00' as timestamp.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ParseUtils.assertTrue(ExportType
                .valueOfIgnoreCase(options.getString(EXPORT_TYPE, null))
                .isInsert(), "Clickhouse only support INSERT export type.");
    }
}
