package com.xiaohongshu.db.hercules.core.option;

import java.util.ArrayList;
import java.util.List;

/**
 * 所有data source的OptionsConf应当继承自此
 */
public class BaseDataSourceOptionsConf extends BaseOptionsConf {

    public static final String DATE_FORMAT = "date-format";
    public static final String TIME_FORMAT = "time-format";
    public static final String DATETIME_FORMAT = "datetime-format";

    public static final String COLUMN = "column";

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String COLUMN_DELIMITER = ",";
    public static final String NESTED_COLUMN_NAME_DELIMITER_REGEX="\\.";


    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(DATE_FORMAT)
                .needArg(true)
                .description(String.format("The date format, default to %s.", DEFAULT_DATE_FORMAT))
                .defaultStringValue(DEFAULT_DATE_FORMAT)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(TIME_FORMAT)
                .needArg(true)
                .description(String.format("The time format, default to %s.", DEFAULT_TIME_FORMAT))
                .defaultStringValue(DEFAULT_TIME_FORMAT)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(DATETIME_FORMAT)
                .needArg(true)
                .description(String.format("The datetime format, default to %s.", DEFAULT_DATETIME_FORMAT))
                .defaultStringValue(DEFAULT_DATETIME_FORMAT)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN)
                .needArg(true)
                .description(String.format("The table column name list, delimited by %s.", COLUMN_DELIMITER))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HELP)
                .needArg(false)
                .description("")
                .build());
        return tmpList;
    }
}
