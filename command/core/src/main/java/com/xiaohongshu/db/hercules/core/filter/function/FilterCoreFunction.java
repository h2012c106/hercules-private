package com.xiaohongshu.db.hercules.core.filter.function;

import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BooleanWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import lombok.NonNull;

public class FilterCoreFunction {

    public static final FilterCoreFunction INSTANCE = new FilterCoreFunction();

    /**
     * 判断是否有null，代表找不到列，普通行为应该是若无此列，一切比大小都是false
     *
     * @param wrappers
     * @return
     */
    private static boolean hasNull(BaseWrapper<?>... wrappers) {
        for (BaseWrapper<?> wrapper : wrappers) {
            if (wrapper == null) {
                return true;
            }
        }
        return false;
    }

    public static BaseWrapper<?> gt(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes > 0);
    }

    public static BaseWrapper<?> gte(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes >= 0);
    }

    public static BaseWrapper<?> lt(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes < 0);
    }

    public static BaseWrapper<?> lte(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes <= 0);
    }

    public static BaseWrapper<?> eq(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes == 0);
    }

    public static BaseWrapper<?> neq(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes != 0);
    }

    public static BaseWrapper<?> properEq(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right) || left.getType() != right.getType()) {
            return BooleanWrapper.get(false);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes == 0);
    }

    public static BaseWrapper<?> properNeq(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        if (left.getType() != right.getType()) {
            return BooleanWrapper.get(true);
        }
        Integer compareRes = left.compareWith(right);
        return BooleanWrapper.get(compareRes != null && compareRes != 0);
    }

    public static BaseWrapper<?> in(BaseWrapper<?> val, BaseWrapper<?> tmp) {
        if (hasNull(val)) {
            return BooleanWrapper.get(false);
        }
        ListWrapper list = (ListWrapper) tmp;
        boolean in = false;
        for (int i = 0; i < list.size(); ++i) {
            if (val.compareWith(list.get(i)) == 0) {
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

    private static CustomDataTypeManager<?, ?> SOURCE_CUSTOM_TYPE_MANAGER = NullCustomDataTypeManager.INSTANCE;

    public static void registerCustomTypeManager(@NonNull CustomDataTypeManager<?, ?> manager) {
        SOURCE_CUSTOM_TYPE_MANAGER = manager;
    }

    /**
     * 避免和druid解析的时候cast函数冲突
     *
     * @param from
     * @param typeNameWrapper
     * @return
     */
    public static BaseWrapper<?> kast(BaseWrapper<?> from, BaseWrapper<?> typeNameWrapper) {
        String typeName = typeNameWrapper.asString();
        DataType dataType = DataType.valueOfIgnoreCase(typeName, SOURCE_CUSTOM_TYPE_MANAGER);
        return dataType.getReadFunction().apply(dataType.getWriteFunction().apply(from));
    }

    public static BaseWrapper<?> pushdown(BaseWrapper<?> wrapper) {
        throw new UnsupportedOperationException();
    }

    public static BaseWrapper<?> notPushdown(BaseWrapper<?> wrapper) {
        throw new UnsupportedOperationException();
    }
}
