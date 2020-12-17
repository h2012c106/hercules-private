package com.xiaohongshu.db.hercules.rdbms.schema;

import java.util.StringJoiner;

public class ColumnInfo {
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int sqlType;
    private final String columnTypeName;

    public ColumnInfo(int sqlType) {
        this.sqlType = sqlType;
        this.signed = true;
        this.precision = -1;
        this.scale = -1;
        this.columnTypeName = null;
    }

    public ColumnInfo(boolean signed, int precision, int scale, int sqlType, String columnTypeName) {
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.sqlType = sqlType;
        this.columnTypeName = columnTypeName;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ColumnInfo.class.getSimpleName() + "[", "]")
                .add("signed=" + signed)
                .add("precision=" + precision)
                .add("scale=" + scale)
                .add("sqlType=" + sqlType)
                .add("columnTypeName='" + columnTypeName + "'")
                .toString();
    }
}
