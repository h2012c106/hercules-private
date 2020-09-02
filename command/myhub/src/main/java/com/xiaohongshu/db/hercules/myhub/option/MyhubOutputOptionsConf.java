package com.xiaohongshu.db.hercules.myhub.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;

import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf.*;

public final class MyhubOutputOptionsConf extends BaseOptionsConf {

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new MysqlOutputOptionsConf(),
                new MyhubOptionsConf()
        );
    }

    @Override
    protected List<String> deleteOptions() {
        return Lists.newArrayList(
                AUTOCOMMIT,
                STATEMENT_PER_COMMIT,
                STAGING_TABLE,
                CLOSE_FORCE_INSERT_STAGING,
                PRE_MIGRATE_SQL
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        return null;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {
    }

    @Override
    protected void innerProcessOptions(GenericOptions options) {
        options.set(AUTOCOMMIT, true);
    }
}
