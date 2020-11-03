package com.xiaohongshu.db.hercules.elasticsearchv7.option;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class ElasticsearchOptionConf extends BaseOptionsConf {

    public static final String ENDPOINT = "endpoint";
    public static final String PORT = "port";
    public static final String INDEX = "index";
    public static final String DOCUMENT_TYPE = "doc-type";
    public static final String KEEP_ID = "keep-id";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(ENDPOINT)
                .needArg(true)
                .necessary(true)
                .description("The endpoint for elasticsearch connection.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(PORT)
                .needArg(true)
                .necessary(true)
                .description("The port for elasticsearch connection.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(INDEX)
                .needArg(true)
                .necessary(true)
                .description("The index for elasticsearch.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(DOCUMENT_TYPE)
                .needArg(true)
                .necessary(true)
                .description("The document type.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(KEEP_ID)
                .needArg(false)
                .defaultStringValue("false")
                .description("Whether keep id or not, default the id will not be passed to values.")
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
