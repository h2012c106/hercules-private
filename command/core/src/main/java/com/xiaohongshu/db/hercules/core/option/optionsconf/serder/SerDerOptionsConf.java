package com.xiaohongshu.db.hercules.core.option.optionsconf.serder;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class SerDerOptionsConf extends BaseOptionsConf {

    public static final String NOT_CONTAINS_KEY = "not-contains-key";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                // 都是序列化结构了，总得是表了吧
                new TableOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(NOT_CONTAINS_KEY)
                .needArg(false)
                .description("Specify whether contains key value in the serialized struct.")
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
