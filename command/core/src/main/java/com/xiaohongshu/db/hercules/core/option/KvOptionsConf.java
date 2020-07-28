package com.xiaohongshu.db.hercules.core.option;

import java.util.ArrayList;
import java.util.List;

public class KvOptionsConf extends BaseOptionsConf {

    public final static String SUPPLIER = "kv-supplier";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(SUPPLIER)
                .needArg(true)
                .description("The supplier to provide key value converter and options.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
