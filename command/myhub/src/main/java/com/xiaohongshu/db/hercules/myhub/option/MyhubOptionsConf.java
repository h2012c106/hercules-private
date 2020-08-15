package com.xiaohongshu.db.hercules.myhub.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOptionsConf;

import java.util.List;

import static com.xiaohongshu.db.hercules.mysql.option.MysqlOptionsConf.IGNORE_AUTOINCREMENT;

public class MyhubOptionsConf extends BaseOptionsConf {
    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new MysqlOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        return null;
    }

    @Override
    protected List<String> deleteOptions() {
        return Lists.newArrayList(
                IGNORE_AUTOINCREMENT
        );
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
