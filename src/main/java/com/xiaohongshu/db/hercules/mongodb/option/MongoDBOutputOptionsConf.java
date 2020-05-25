package com.xiaohongshu.db.hercules.mongodb.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.mongodb.ExportType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN_DELIMITER;
import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN_TYPE;

public final class MongoDBOutputOptionsConf extends BaseOptionsConf {

    public static final String OBJECT_ID = "object-id";
    public static final String EXPORT_TYPE = "export-type";
    public static final String UPDATE_KEY = "update-key";
    public static final String UPSERT = "upsert";
    public static final String STATEMENT_PER_BULK = "statement-per-bulk";
    public static final String BULK_ORDERED = "bulk-ordered";
    public static final String EXECUTE_THREAD_NUM = "execute-thread-num";
    public static final String DECIMAL_AS_STRING = "decimal-as-string";

    private static final String[] DEFAULT_OBJECT_ID = new String[]{"_id"};
    private static final long DEFAULT_STATEMENT_PER_BULK = 200;
    public static final int DEFAULT_EXECUTE_THREAD_NUM = 1;

    private static final String OBJECT_ID_DELIMITER = ",";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseOutputOptionsConf(),
                new MongoDBOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(OBJECT_ID)
                .needArg(true)
                .description(String.format("The objectId type column list, delimited by %s. " +
                        "This type config will overwrite the %s config", OBJECT_ID_DELIMITER, COLUMN_TYPE))
                .defaultStringValue(String.join(OBJECT_ID_DELIMITER, DEFAULT_OBJECT_ID))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EXPORT_TYPE)
                .needArg(true)
                .necessary(true)
                .description(String.format("The export type: %s.", Arrays.stream(ExportType.values())
                        .map(Enum::name)
                        .collect(Collectors.joining(" / "))))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(UPDATE_KEY)
                .needArg(true)
                .description(String.format("The column update/replace mode used as filter, delimited by %s. " +
                                "Must exist in source data.",
                        COLUMN_DELIMITER))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(UPSERT)
                .needArg(false)
                .description("Whether insert if update/replace operation cover zero line.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(STATEMENT_PER_BULK)
                .needArg(true)
                .description("The statement num each bulk submits.")
                .defaultStringValue(Long.toString(DEFAULT_STATEMENT_PER_BULK))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(BULK_ORDERED)
                .needArg(false)
                .description("If specified, will use ordered bulk write mode. " +
                        "If you want to learn more, check https://docs.mongodb.com/manual/core/bulk-write-operations/#ordered-vs-unordered-operations.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EXECUTE_THREAD_NUM)
                .needArg(true)
                .description("The thread num when executing update statement.")
                .defaultStringValue(Integer.toString(DEFAULT_EXECUTE_THREAD_NUM))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(DECIMAL_AS_STRING)
                .needArg(false)
                .description("The decimal is not supported until v3.4.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ExportType exportType = ExportType.valueOfIgnoreCase(options.getString(EXPORT_TYPE, null));
        if (!exportType.isInsert()) {
            ParseUtils.assertTrue(options.hasProperty(UPDATE_KEY), "When use update/replace mode, the update key must be specified.");
        }
        if (options.getBoolean(UPSERT, false)) {
            ParseUtils.assertTrue(!exportType.isInsert(), "Why insert mode need upsert feature.");
        }
        ParseUtils.assertTrue(options.getInteger(STATEMENT_PER_BULK, null) > 0,
                "The statement num per bulk should > 0.");
        ParseUtils.assertTrue(options.getInteger(EXECUTE_THREAD_NUM, null) > 0,
                "The thread num should > 0.");
    }
}
