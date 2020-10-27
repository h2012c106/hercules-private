package com.xiaohongshu.db.hercules.elasticsearch.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class ElasticsearchOutputOptionConf extends BaseOptionsConf {

    public static final String ID_COL_NAME = "id-col-name";
    public static final String BUFFER_SIZE = "buffer-size";
    public static final String DEFAULT_BUFFER_SIZE = "1000";

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
        tmpList.add(SingleOptionConf.builder()
                .name(BUFFER_SIZE)
                .defaultStringValue(DEFAULT_BUFFER_SIZE)
                .needArg(true)
                .description("The buffer size used for elasticsearch bulk request.")
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
