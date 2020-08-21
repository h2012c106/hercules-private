package com.xiaohongshu.db.hercules.core.option.optionsconf.serder;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;

import java.util.List;

public class SerOptionsConf extends BaseOptionsConf {
    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new SerDerOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        return null;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
