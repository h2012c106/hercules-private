package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.List;

public class CanalMysqlInputOptionConf extends BaseOptionsConf {
    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        return null;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
