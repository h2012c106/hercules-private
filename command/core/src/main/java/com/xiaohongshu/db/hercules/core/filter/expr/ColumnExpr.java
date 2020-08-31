package com.xiaohongshu.db.hercules.core.filter.expr;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;

public class ColumnExpr extends AbstractExpr {

    private final String columnName;

    public ColumnExpr(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public BaseWrapper<?> getResult(HerculesWritable row) {
        return WritableUtils.get(row.getRow(), columnName);
    }

    @Override
    public String toString() {
        return "COLUMN<" + columnName + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnExpr that = (ColumnExpr) o;
        return Objects.equal(columnName, that.columnName);
    }
}
