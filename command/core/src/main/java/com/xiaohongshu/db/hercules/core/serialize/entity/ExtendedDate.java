package com.xiaohongshu.db.hercules.core.serialize.entity;

import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import lombok.NonNull;

import java.util.Date;

public class ExtendedDate {
    private final Date date;
    private final boolean isZero;

    public static ExtendedDate initialize(@NonNull String date){
        return new ExtendedDate(DateUtils.stringToDate(date, DateUtils.getSourceDateFormat()));
    }

    public static ExtendedDate initialize(@NonNull Long date){
        return new ExtendedDate(new Date(date));
    }

    public static ExtendedDate initialize(@NonNull Date date){
        return new ExtendedDate(date);
    }

    public static ExtendedDate ZERO_INSTANCE = new ExtendedDate();

    private ExtendedDate(Date date) {
        this.date = date;
        this.isZero = false;
    }

    private ExtendedDate() {
        this.date = null;
        this.isZero = true;
    }

    public Date getDate() {
        if (isZero) {
            throw new RuntimeException("Unable to get date due to the 0000-00-00 00:00:00 case.");
        } else {
            return date;
        }
    }

    public boolean isZero() {
        return isZero;
    }
}
