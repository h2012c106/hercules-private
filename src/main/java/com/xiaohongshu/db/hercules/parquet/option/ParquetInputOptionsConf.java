package com.xiaohongshu.db.hercules.parquet.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.common.option.CommonOptionsConf.NUM_MAPPER;

public final class ParquetInputOptionsConf extends BaseOptionsConf {

    public static final String TASK_SIDE_METADATA = "task-side-metadata";
    public static final String ORIGINAL_SPLIT = "original-split";
    public static final String EMPTY_AS_NULL = "empty-as-null";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseInputOptionsConf(),
                new ParquetOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(TASK_SIDE_METADATA)
                .needArg(false)
                .description("If specified, turn on task side metadata loading." +
                        " if on then metadata is read on the task side and some tasks may finish immediately." +
                        " if off then metadata is read on the client which is slower if there is a lot of metadata but tasks will only be spawn if there is work to do." +
                        " (this description is copied from parquet source code comment)")
                .build());
        res.add(SingleOptionConf.builder()
                .name(ORIGINAL_SPLIT)
                .needArg(false)
                .description(String.format("If specified, turn on the parquet original split strategy " +
                        "which depends on the file num and block num, and the '--%s' will be ignored.", NUM_MAPPER))
                .build());
        res.add(SingleOptionConf.builder()
                .name(EMPTY_AS_NULL)
                .needArg(false)
                .description(String.format("The optional empty value handling mode, if specified, the empty value will be treated as null. " +
                        "e.g. If downstream is rdbms, null value will insert null; empty value will insert default. " +
                        "This switch will only be activated when choose '%s' schema style.", SchemaStyle.ORIGINAL))
                .build());
        return res;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
