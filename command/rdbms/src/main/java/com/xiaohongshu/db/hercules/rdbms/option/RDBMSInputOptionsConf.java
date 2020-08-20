package com.xiaohongshu.db.hercules.rdbms.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf.COLUMN_DELIMITER;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf.TABLE;

public final class RDBMSInputOptionsConf extends BaseOptionsConf {

    public static final String CONDITION = "condition";
    public static final String QUERY = "query";

    public static final String SPLIT_BY = "split-by";
    public static final String SPLIT_BY_HEX_STRING = "split-by-hex-string";
    public static final String BALANCE_SPLIT = "balance";
    public static final String RANDOM_FUNC_NAME = "random-func-name";
    public static final String BALANCE_SPLIT_SAMPLE_MAX_ROW = "balance-sample-max-row";
    public static final String FETCH_SIZE = "fetch-size";
    public static final String IGNORE_SPLIT_KEY_CHECK = "ignore-split-key-check";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseInputOptionsConf(),
                new RDBMSOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(TABLE)
                .needArg(true)
                .description("The source table name.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(CONDITION)
                .needArg(true)
                .description(String.format("The 'where' condition applied to source table, " +
                        "should used with '%s' param.", TABLE))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN)
                .needArg(true)
                .description(String.format("The source table column name list, delimited by %s. " +
                                "In '%s' mode, it's optional; In '%s' mode, it's necessary.",
                        COLUMN_DELIMITER, TABLE, QUERY))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(QUERY)
                .needArg(true)
                .description("The query sql used on source database.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SPLIT_BY)
                .needArg(true)
                .description("The column that splitting map will depend on. " +
                        "If not set, will try to find primary key and use it.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(RANDOM_FUNC_NAME)
                .needArg(true)
                .description("The random function for temp database, [0, 1], allow any database-supported expression.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SPLIT_BY_HEX_STRING)
                .needArg(false)
                .description("If the split-by column is string type, it will be treated as hex to split.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(BALANCE_SPLIT)
                .needArg(false)
                .description("If the split-by column is not capable to avoid skewing, " +
                        "the balance mode will sampling on the split-by column " +
                        "and find the exact split point from the sampled column data.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(BALANCE_SPLIT_SAMPLE_MAX_ROW)
                .needArg(true)
                .description("The sample max row num when use balance mode, " +
                        "to deal with the may-happening oom case.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(FETCH_SIZE)
                .needArg(true)
                .description("The select fetch size, if not specified will not use this feature.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(IGNORE_SPLIT_KEY_CHECK)
                .needArg(false)
                .description("If specified, will not abandon the situation that specifying a non-key column as split key.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ParseUtils.validateDependency(options,
                null,
                null,
                null,
                Lists.newArrayList(TABLE, QUERY));
        ParseUtils.validateDependency(options,
                CONDITION,
                null,
                Lists.newArrayList(TABLE),
                null);
        ParseUtils.validateDependency(options,
                BALANCE_SPLIT_SAMPLE_MAX_ROW,
                null,
                Lists.newArrayList(BALANCE_SPLIT),
                null);

        Integer splitMaxRow = options.getInteger(BALANCE_SPLIT_SAMPLE_MAX_ROW, null);
        if (splitMaxRow != null) {
            ParseUtils.assertTrue(splitMaxRow > 0,
                    "Illegal balance split max row num: " + splitMaxRow);
        }

        Integer fetchSize = options.getInteger(FETCH_SIZE, null);
        if (fetchSize != null) {
            ParseUtils.assertTrue(fetchSize > 0,
                    "Illegal fetch size value: " + fetchSize);
        }

        String splitBy = options.getString(SPLIT_BY, null);
        if (splitBy != null) {
            ParseUtils.assertTrue(!splitBy.contains(","),
                    "Unsupported to use multiple split-by key.");
        }

        if (options.getBoolean(BALANCE_SPLIT, false)) {
            ParseUtils.assertTrue(options.hasProperty(RANDOM_FUNC_NAME),
                    String.format("If you are using balance mode, " +
                                    "please use '--%s' to specify the random function name to enable random sampling.",
                            RANDOM_FUNC_NAME));
        }
    }
}
