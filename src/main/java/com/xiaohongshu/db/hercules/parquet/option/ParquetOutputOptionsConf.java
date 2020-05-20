package com.xiaohongshu.db.hercules.parquet.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class ParquetOutputOptionsConf extends BaseOptionsConf {

    public static final String COMPRESSION_CODEC = "compression-codec";
    public static final String EMPTY_AS_NULL = "empty-as-null";
    public static final String DELETE_TARGET_DIR = "delete-target-dir";

    private static final CompressionCodecName DEFAULT_COMPRESSION_CODEC = CompressionCodecName.SNAPPY;

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseOutputOptionsConf(),
                new ParquetOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(COMPRESSION_CODEC)
                .needArg(true)
                .description(String.format("The compression codec when writing: %s, default to: %s.",
                        Arrays.stream(CompressionCodecName.values()).map(CompressionCodecName::name).collect(Collectors.joining(" / ")),
                        DEFAULT_COMPRESSION_CODEC.name()))
                .defaultStringValue(DEFAULT_COMPRESSION_CODEC.name())
                .build());
        res.add(SingleOptionConf.builder()
                .name(EMPTY_AS_NULL)
                .needArg(false)
                .description(String.format("The optional empty value handling mode, if specified, the empty value will be treated as null. " +
                        "e.g. If downstream is rdbms, null value will insert null; empty value will insert default. " +
                        "This switch will only be activated when choose '%s' schema style.", SchemaStyle.ORIGINAL))
                .build());
        res.add(SingleOptionConf.builder()
                .name(DELETE_TARGET_DIR)
                .needArg(false)
                .description("If specified, will recursively delete target dir.")
                .build());
        return res;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
