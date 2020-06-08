package com.xiaohongshu.db.hercules.core.option;

import java.util.Collections;
import java.util.List;

public class BaseOutputOptionsConf extends BaseOptionsConf {
    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Collections.singletonList(new BaseDataSourceOptionsConf());
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        return null;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
