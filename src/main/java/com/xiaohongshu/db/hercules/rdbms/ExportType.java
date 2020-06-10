package com.xiaohongshu.db.hercules.rdbms;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

public enum ExportType {
    INSERT,
    UPDATE,
    INSERT_IGNORE,
    UPSERT,
    REPLACE;

    public boolean isInsert() {
        return INSERT.equals(this);
    }

    public boolean isUpdate() {
        return UPDATE.equals(this);
    }

    public boolean isInsertIgnore() {
        return INSERT_IGNORE.equals(this);
    }

    public boolean isUpsert() {
        return UPSERT.equals(this);
    }

    public boolean isReplace() {
        return REPLACE.equals(this);
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
