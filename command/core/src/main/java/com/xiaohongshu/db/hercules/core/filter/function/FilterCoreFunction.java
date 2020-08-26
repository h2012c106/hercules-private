package com.xiaohongshu.db.hercules.core.filter.function;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BooleanWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;

import java.util.Arrays;

public class FilterCoreFunction {

    public static final FilterCoreFunction INSTANCE = new FilterCoreFunction();

    public static BaseWrapper<?> gt(BaseWrapper<?> left, BaseWrapper<?> right) {
        return BooleanWrapper.get(left.compareTo(right) > 0);
    }

    public static BaseWrapper<?> gte(BaseWrapper<?> left, BaseWrapper<?> right) {
        return BooleanWrapper.get(left.compareTo(right) >= 0);
    }

    public static BaseWrapper<?> lt(BaseWrapper<?> left, BaseWrapper<?> right) {
        return BooleanWrapper.get(left.compareTo(right) < 0);
    }

    public static BaseWrapper<?> lte(BaseWrapper<?> left, BaseWrapper<?> right) {
        return BooleanWrapper.get(left.compareTo(right) <= 0);
    }

    public static BaseWrapper<?> eq(BaseWrapper<?> left, BaseWrapper<?> right) {
        return BooleanWrapper.get(left.equals(right));
    }

    public static BaseWrapper<?> neq(BaseWrapper<?> left, BaseWrapper<?> right) {
        return BooleanWrapper.get(!left.equals(right));
    }

    public static BaseWrapper<?> in(BaseWrapper<?> val, BaseWrapper<?> tmp) {
        ListWrapper list = (ListWrapper) tmp;
        boolean in = false;
        for (int i = 0; i < list.size(); ++i) {
            if (val.compareTo(list.get(i)) == 0) {
                in = true;
                break;
            }
        }
        return BooleanWrapper.get(in);
    }

    public static BaseWrapper<?> len(BaseWrapper<?> val) {
        if (val instanceof ListWrapper) {
            return IntegerWrapper.get(((ListWrapper) val).size());
        } else {
            return IntegerWrapper.get(val.asString().length());
        }
    }
}
