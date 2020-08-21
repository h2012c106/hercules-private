package com.xiaohongshu.db.hercules.core.utils;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class DateUtils {

    private static final Log LOG = LogFactory.getLog(DateUtils.class);

    public static final String ZERO_DATE = "0000-00-00 00:00:00";

    /**
     * 用于应对上下游时间格式不一致
     */
    private static DateFormatWrapper sourceDateFormat;
    private static DateFormatWrapper targetDateFormat;

    private static ThreadLocal<DateFormatterWrapper> sourceDateFormatterWrapper = new ThreadLocal<DateFormatterWrapper>() {
        @Override
        protected DateFormatterWrapper initialValue() {
            return new DateFormatterWrapper(sourceDateFormat);
        }
    };
    private static ThreadLocal<DateFormatterWrapper> targetDateFormatterWrapper = new ThreadLocal<DateFormatterWrapper>() {
        @Override
        protected DateFormatterWrapper initialValue() {
            return new DateFormatterWrapper(targetDateFormat);
        }
    };

    public static void setFormats(GenericOptions sourceOptions, GenericOptions targetOptions) {
        DateUtils.sourceDateFormat = new DateFormatWrapper(
                sourceOptions.getString(BaseDataSourceOptionsConf.DATE_FORMAT, null),
                sourceOptions.getString(BaseDataSourceOptionsConf.TIME_FORMAT, null),
                sourceOptions.getString(BaseDataSourceOptionsConf.DATETIME_FORMAT, null));
        DateUtils.targetDateFormat = new DateFormatWrapper(
                targetOptions.getString(BaseDataSourceOptionsConf.DATE_FORMAT, null),
                targetOptions.getString(BaseDataSourceOptionsConf.TIME_FORMAT, null),
                targetOptions.getString(BaseDataSourceOptionsConf.DATETIME_FORMAT, null));
    }

    public static DateFormatterWrapper getSourceDateFormat() {
        return sourceDateFormatterWrapper.get();
    }

    public static DateFormatterWrapper getTargetDateFormat() {
        return targetDateFormatterWrapper.get();
    }

    public static Date stringToDate(String value, BaseDataType dateType, DateFormatterWrapper dateFormatterWrapper) {
        try {
            switch (dateType) {
                case DATE:
                    return dateFormatterWrapper.getDateFormat().parse(value);
                case TIME:
                    return dateFormatterWrapper.getTimeFormat().parse(value);
                case DATETIME:
                    return dateFormatterWrapper.getDatetimeFormat().parse(value);
                default:
                    throw new RuntimeException("Unknown date type: " + dateType);
            }
        } catch (ParseException e) {
            throw new SerializeException(String.format("Unparsable formatted date [%s], exception: %s",
                    value, ExceptionUtils.getStackTrace(e)));
        }
    }

    public static Date stringToDate(String value, DateFormatterWrapper dateFormatterWrapper) {
        try {
            return dateFormatterWrapper.getDatetimeFormat().parse(value);
        } catch (ParseException ignored) {
        }
        try {
            return dateFormatterWrapper.getDateFormat().parse(value);
        } catch (ParseException ignored) {
        }
        try {
            return dateFormatterWrapper.getTimeFormat().parse(value);
        } catch (ParseException ignored) {
        }
        throw new SerializeException("Unparsable formatted date: " + value);
    }

    public static String timestampToString(long value, BaseDataType dateType, DateFormatterWrapper dateFormatterWrapper) {
        SimpleDateFormat format;
        switch (dateType) {
            case DATE:
                format = dateFormatterWrapper.getDateFormat();
                break;
            case TIME:
                format = dateFormatterWrapper.getTimeFormat();
                break;
            case DATETIME:
                format = dateFormatterWrapper.getDatetimeFormat();
                break;
            default:
                throw new RuntimeException("Unknown date type: " + dateType);
        }
        return format.format(value);
    }

    public static String dateToString(Date value, BaseDataType dateType, DateFormatterWrapper dateFormatterWrapper) {
        SimpleDateFormat format;
        switch (dateType) {
            case DATE:
                format = dateFormatterWrapper.getDateFormat();
                break;
            case TIME:
                format = dateFormatterWrapper.getTimeFormat();
                break;
            case DATETIME:
                format = dateFormatterWrapper.getDatetimeFormat();
                break;
            default:
                throw new RuntimeException("Unknown date type: " + dateType);
        }
        return format.format(value);
    }

    public static class DateResult {
        private Date value;
        private BaseDataType type;

        public DateResult(Date value, BaseDataType type) {
            this.value = value;
            this.type = type;
        }

        public Date getValue() {
            return value;
        }

        public BaseDataType getType() {
            return type;
        }
    }

    private static class DateFormatWrapper {
        private String dateFormat;
        private String timeFormat;
        private String datetimeFormat;

        public DateFormatWrapper(String dateFormat, String timeFormat, String datetimeFormat) {
            this.dateFormat = dateFormat;
            this.timeFormat = timeFormat;
            this.datetimeFormat = datetimeFormat;
        }

        public String getDateFormat() {
            return dateFormat;
        }

        public String getTimeFormat() {
            return timeFormat;
        }

        public String getDatetimeFormat() {
            return datetimeFormat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DateFormatWrapper that = (DateFormatWrapper) o;
            return Objects.equal(dateFormat, that.dateFormat) &&
                    Objects.equal(timeFormat, that.timeFormat) &&
                    Objects.equal(datetimeFormat, that.datetimeFormat);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dateFormat, timeFormat, datetimeFormat);
        }
    }

    public static class DateFormatterWrapper {
        private SimpleDateFormat dateFormat;
        private SimpleDateFormat timeFormat;
        private SimpleDateFormat datetimeFormat;

        public DateFormatterWrapper(DateFormatWrapper dateFormatWrapper) {
            this.dateFormat = new SimpleDateFormat(dateFormatWrapper.getDateFormat());
            this.timeFormat = new SimpleDateFormat(dateFormatWrapper.getTimeFormat());
            this.datetimeFormat = new SimpleDateFormat(dateFormatWrapper.getDatetimeFormat());

            this.dateFormat.setLenient(false);
            this.timeFormat.setLenient(false);
            this.datetimeFormat.setLenient(false);
        }

        public SimpleDateFormat getDateFormat() {
            return dateFormat;
        }

        public SimpleDateFormat getTimeFormat() {
            return timeFormat;
        }

        public SimpleDateFormat getDatetimeFormat() {
            return datetimeFormat;
        }
    }
}
