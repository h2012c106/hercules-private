package com.xiaohongshu.db.hercules.core.option;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 所有data source的OptionsConf应当继承自此
 */
public final class BaseDataSourceOptionsConf extends BaseOptionsConf {

    public static final String DATE_FORMAT = "date-format";
    public static final String TIME_FORMAT = "time-format";
    public static final String DATETIME_FORMAT = "datetime-format";

    public static final String COLUMN = "column";
    public static final String COLUMN_TYPE = "column-type";

    private final static JSONObject DEFAULT_COLUMN_TYPE = new JSONObject();

    private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String COLUMN_DELIMITER = ",";
    public static final String NESTED_COLUMN_NAME_DELIMITER = ".";
    public static final String NESTED_COLUMN_NAME_DELIMITER_REGEX = Pattern.quote(NESTED_COLUMN_NAME_DELIMITER);

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
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
                .name(COLUMN_TYPE)
                .needArg(true)
                .description(String.format("The table column type map, formatted in json, type: %s.", Arrays.toString(BaseDataType.values())))
                .defaultStringValue(DEFAULT_COLUMN_TYPE.toJSONString())
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HELP)
                .needArg(false)
                .description("")
                .build());
        return tmpList;
    }

    private void validateDateFormat(String format, String formatType) {
        try {
            FastDateFormat.getInstance(format);
        } catch (Exception e) {
            throw new ParseException(String.format("Unable to parse %s as %s format, due to: %s",
                    format, formatType, ExceptionUtils.getStackTrace(e)));
        }
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        validateDateFormat(options.getString(BaseDataSourceOptionsConf.DATE_FORMAT, null),
                "date");
        validateDateFormat(options.getString(BaseDataSourceOptionsConf.TIME_FORMAT, null),
                "time");
        validateDateFormat(options.getString(BaseDataSourceOptionsConf.DATETIME_FORMAT, null),
                "datetime");

        if (options.hasProperty(BaseDataSourceOptionsConf.COLUMN)) {
            ParseUtils.assertTrue(options.getStringArray(BaseDataSourceOptionsConf.COLUMN, null).length > 0,
                    "It's meaningless to set a zero-length column name list.");
        }
    }
}
