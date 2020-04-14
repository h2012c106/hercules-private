package com.xiaohongshu.db.hercules.rdbms.option;

import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.List;

public class RDBMSInputOptionsConf extends RDBMSOptionsConf {

    public static final String CONDITION = "condition";
    public static final String QUERY = "query";

    public static final String SPLIT_BY = "split-by";
    public static final String SPLIT_BY_HEX_STRING = "split-by-hex-string";
    public static final String BALANCE_SPLIT = "balance";
    public static final String RANDOM_FUNC_NAME = "random-func-name";
    public static final String BALANCE_SPLIT_SAMPLE_MAX_ROW = "balance-sample-max-row";
    public static final String FETCH_SIZE = "fetch-size";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
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
                .description("The select fetch size.")
                .build());
        return tmpList;
    }
}
