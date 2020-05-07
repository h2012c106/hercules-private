package com.xiaohongshu.db.hercules.mongodb;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

public enum ExportType {
    INSERT,
    UPDATE_ONE,
    UPDATE_MANY,
    REPLACE_ONE;

    public boolean isInsert(){
        return INSERT.equals(this);
    }

    public boolean isUpdateOne(){
        return UPDATE_ONE.equals(this);
    }

    public boolean isUpdateMany(){
        return UPDATE_MANY.equals(this);
    }

    public boolean isReplaceOne(){
        return REPLACE_ONE.equals(this);
    }

    public static ExportType valueOfIgnoreCase(String value) {
        for (ExportType exportType : ExportType.values()) {
            if (StringUtils.equalsIgnoreCase(exportType.name(), value)) {
                return exportType;
            }
        }
        throw new ParseException("Illegal export type: " + value);
    }
}
