package com.xiaohongshu.db.hercules.core.filter.expr;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.filter.function.FilterCoreFunction;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

public class ValueExpr extends AbstractExpr {
    /**
     * 这里肯定不能支持特殊数据类型，filter是给大家用的，且parser是druid，也parse不出特殊类型
     */
    private final DataType dataType;
    private final BaseWrapper<?> value;

    public ValueExpr(Object value, DataType dataType) {
        this.dataType = dataType;
        this.value = dataType.getReadFunction().apply(value);
    }

    public ValueExpr(BaseWrapper<?> wrapper) {
        this.dataType = wrapper.getType();
        this.value = wrapper;
    }

    public DataType getDataType() {
        return dataType;
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
                FilterCoreFunction.properEq(value, valueExpr.value).asBoolean();
    }
}
