package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;

/**
 * 所有data source的parser应当继承自此
 */
public abstract class BaseDataSourceParser extends BaseParser<BaseDataSourceOptionsConf> {

    private void validateDateFormat(String format, String formatType) {
        try {
            FastDateFormat.getInstance(format);
        } catch (Exception e) {
            throw new ParseException(String.format("Unable to parse %s as %s format, due to: %s",
                    format, formatType, ExceptionUtils.getStackTrace(e)));
        }
    }

    @Override
    protected void validateOptions(GenericOptions options) {
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
