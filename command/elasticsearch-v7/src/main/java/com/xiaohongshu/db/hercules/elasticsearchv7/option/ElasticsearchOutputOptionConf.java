package com.xiaohongshu.db.hercules.elasticsearchv7.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class ElasticsearchOutputOptionConf extends BaseOptionsConf {

    public static final String ID_COL_NAME = "id-col-name";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new ElasticsearchOptionConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(ID_COL_NAME)
                .needArg(true)
                .necessary(true)
                .description("The column used as id in elasticsearch index.")
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
