package com.xiaohongshu.db.hercules.parquet.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ParquetOptionsConf extends BaseOptionsConf {

    public static final String DIR = "dir";
    public static final String MESSAGE_TYPE = "message-type";
    public static final String SCHEMA_STYLE = "schema-style";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new TableOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(DIR)
                .needArg(true)
                .necessary(true)
                .description("The parquet files' directory.")
                .build());
        res.add(SingleOptionConf.builder()
                .name(MESSAGE_TYPE)
                .needArg(true)
                .necessary(false)
                .description("The parquet files' schema.")
                .build());
        res.add(SingleOptionConf.builder()
                .name(SCHEMA_STYLE)
                .needArg(true)
                .necessary(true)
                .description(String.format("The parquet files' schema style according to the logic type (column type): %s.",
                        Arrays.stream(SchemaStyle.values()).map(SchemaStyle::name).collect(Collectors.joining(" / "))))
                .build());
        return res;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
