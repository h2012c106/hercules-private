package com.xiaohongshu.db.hercules.parquetschema.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class ParquetSchemaOptionsConf extends BaseOptionsConf {

    public static final String TRY_REQUIRED = "try-required";
    public static final String TYPE_AUTO_UPGRADE = "type-auto-upgrade";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new ParquetOptionsConf(),
                new BaseOutputOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(TRY_REQUIRED)
                .needArg(false)
                .description("If specified, will try best to recognize the required field which exists in every row.")
                .build());
        res.add(SingleOptionConf.builder()
                .name(TYPE_AUTO_UPGRADE)
                .needArg(false)
                .description("If specified, will try to upgrade the lower grade type to higher grade, e.g. INTEGER->LONG.")
                .build());
        return res;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
