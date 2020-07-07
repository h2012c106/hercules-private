package com.xiaohongshu.db.hercules.core.option;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.List;

public class KvOptionsConf extends BaseOptionsConf {

    public final static String SUPPLIER = "kv-supplier";
    public final static String DEFAULT_SUPPLIER = "com.xiaohongshu.db.hercules.converter.blank.BlankKvConverterSupplier";

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
                .defaultStringValue(DEFAULT_SUPPLIER)
                .description("The supplier to provide key value converter and options.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
