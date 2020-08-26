package com.xiaohongshu.db.hercules.core.filter.expr;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

import java.util.List;

public class ValueExpr extends AbstractExpr {
    /**
     * 这里肯定不能支持特殊数据类型，filter是给大家用的，且parser是druid，也parse不出特殊类型
     */
    private final BaseDataType dataType;
    private final BaseWrapper<?> value;

    public ValueExpr(Object value, BaseDataType dataType) {
        this.dataType = dataType;
        this.value = dataType.getReadFunction().apply(value);
    }

    public Object getValue() {
        return dataType.getWriteFunction().apply(value);
    }

    public BaseWrapper<?> getResult() {
        return value;
    }

    @Override
    public BaseWrapper<?> getResult(HerculesWritable row) {
        return getResult();
    }

    @Override
    public String toString() {
        return "VALUE<" + value + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueExpr valueExpr = (ValueExpr) o;
        return dataType == valueExpr.dataType &&
                Objects.equal(value, valueExpr.value);
    }
}
