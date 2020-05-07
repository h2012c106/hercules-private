package com.xiaohongshu.db.hercules.core.utils;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class DateUtils {

    /**
     * 用于应对上下游时间格式不一致
     */
    private static DateUtils.DateFormatWrapper sourceDateFormat;
    private static DateUtils.DateFormatWrapper targetDateFormat;

    public static void setFormats(GenericOptions sourceOptions, GenericOptions targetOptions) {
        DateUtils.sourceDateFormat = new DateUtils.DateFormatWrapper(
                sourceOptions.getString(BaseDataSourceOptionsConf.DATE_FORMAT, null),
                sourceOptions.getString(BaseDataSourceOptionsConf.TIME_FORMAT, null),
                sourceOptions.getString(BaseDataSourceOptionsConf.DATETIME_FORMAT, null));
        DateUtils.targetDateFormat = new DateUtils.DateFormatWrapper(
                targetOptions.getString(BaseDataSourceOptionsConf.DATE_FORMAT, null),
                targetOptions.getString(BaseDataSourceOptionsConf.TIME_FORMAT, null),
                targetOptions.getString(BaseDataSourceOptionsConf.DATETIME_FORMAT, null));
    }

    public static DateFormatWrapper getSourceDateFormat() {
        return sourceDateFormat;
    }

    public static DateFormatWrapper getTargetDateFormat() {
        return targetDateFormat;
    }

    public static DateResult stringToDate(String value, DateFormatWrapper dateFormatWrapper) {
        try {
            return new DateResult(dateFormatWrapper.getDatetimeFormat().parse(value), DateType.DATETIME);
        } catch (ParseException ignored) {
        }
        try {
            return new DateResult(dateFormatWrapper.getDateFormat().parse(value), DateType.DATE);
        } catch (ParseException ignored) {
        }
        try {
            return new DateResult(dateFormatWrapper.getTimeFormat().parse(value), DateType.TIME);
        } catch (ParseException ignored) {
        }
        throw new SerializeException("Unparsable formatted date: " + value);
    }

    public static String timestampToString(long value, DateType dateType, DateFormatWrapper dateFormatWrapper) {
        SimpleDateFormat format;
        switch (dateType) {
            case DATE:
                format = dateFormatWrapper.getDateFormat();
                break;
            case TIME:
                format = dateFormatWrapper.getTimeFormat();
                break;
            default:
                format = dateFormatWrapper.getDatetimeFormat();
        }
        return format.format(value);
    }

    public static String dateToString(Date value, DateType dateType, DateFormatWrapper dateFormatWrapper) {
        SimpleDateFormat format;
        switch (dateType) {
            case DATE:
                format = dateFormatWrapper.getDateFormat();
                break;
            case TIME:
                format = dateFormatWrapper.getTimeFormat();
                break;
            default:
                format = dateFormatWrapper.getDatetimeFormat();
        }
        return format.format(value);
    }

    public enum DateType {
        DATE,
        TIME,
        DATETIME;
    }

    public static class DateResult {
        private Date value;
        private DateType type;

        public DateResult(Date value, DateType type) {
            this.value = value;
            this.type = type;
        }

        public Date getValue() {
            return value;
        }

        public DateType getType() {
            return type;
        }
    }

    public static class DateFormatWrapper {
        private SimpleDateFormat dateFormat;
        private SimpleDateFormat timeFormat;
        private SimpleDateFormat datetimeFormat;

        public DateFormatWrapper(String dateFormat, String timeFormat, String datetimeFormat) {
            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.timeFormat = new SimpleDateFormat(timeFormat);
            this.datetimeFormat = new SimpleDateFormat(datetimeFormat);

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DateFormatWrapper that = (DateFormatWrapper) o;
            return Objects.equal(dateFormat.toPattern(), that.dateFormat.toPattern()) &&
                    Objects.equal(timeFormat.toPattern(), that.timeFormat.toPattern()) &&
                    Objects.equal(datetimeFormat.toPattern(), that.datetimeFormat.toPattern());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dateFormat.toPattern(), timeFormat.toPattern(), datetimeFormat.toPattern());
        }
    }
}
