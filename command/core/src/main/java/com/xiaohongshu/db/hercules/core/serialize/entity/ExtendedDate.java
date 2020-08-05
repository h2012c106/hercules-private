package com.xiaohongshu.db.hercules.core.serialize.entity;

import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import lombok.NonNull;

import java.util.Date;

public class ExtendedDate {
    private final Date date;
    private final boolean isZero;

    public ExtendedDate(@NonNull String date) {
        this.date = DateUtils.stringToDate(date, DateUtils.getSourceDateFormat());
        this.isZero = false;
    }

    public ExtendedDate(@NonNull Long date) {
        this.date = new Date(date);
        this.isZero = false;
    }

    public ExtendedDate(@NonNull Date date) {
        this.date = date;
        this.isZero = false;
    }

    public ExtendedDate() {
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
