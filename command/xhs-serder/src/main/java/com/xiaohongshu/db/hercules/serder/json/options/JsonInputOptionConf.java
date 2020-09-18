package com.xiaohongshu.db.hercules.serder.json.options;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.serder.DerOptionsConf;

import java.util.List;

public class JsonInputOptionConf extends BaseOptionsConf {

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new DerOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        return null;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
