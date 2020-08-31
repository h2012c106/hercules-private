package com.xiaohongshu.db.hercules.core.filter.function;

import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BooleanWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;

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
        boolean res;
        try {
            res = left.compareTo(right) > 0;
        } catch (Exception ignored) {
            // 不可比则这行过不了筛查，类似mongo的处理方式，类型不同则不会被filter出来
            res = false;
        }
        return BooleanWrapper.get(res);
    }

    public static BaseWrapper<?> gte(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        boolean res;
        try {
            res = left.compareTo(right) >= 0;
        } catch (Exception ignored) {
            // 不可比则这行过不了筛查，类似mongo的处理方式，类型不同则不会被filter出来
            res = false;
        }
        return BooleanWrapper.get(res);
    }

    public static BaseWrapper<?> lt(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        boolean res;
        try {
            res = left.compareTo(right) < 0;
        } catch (Exception ignored) {
            // 不可比则这行过不了筛查，类似mongo的处理方式，类型不同则不会被filter出来
            res = false;
        }
        return BooleanWrapper.get(res);
    }

    public static BaseWrapper<?> lte(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        boolean res;
        try {
            res = left.compareTo(right) <= 0;
        } catch (Exception ignored) {
            // 不可比则这行过不了筛查，类似mongo的处理方式，类型不同则不会被filter出来
            res = false;
        }
        return BooleanWrapper.get(res);
    }

    public static BaseWrapper<?> eq(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        boolean res;
        if (left.isNull() && right.isNull()) {
            res = true;
        } else if (left.isNull() || right.isNull()) {
            res = false;
        } else {
            res = left.equals(right);
        }
        return BooleanWrapper.get(res);
    }

    public static BaseWrapper<?> neq(BaseWrapper<?> left, BaseWrapper<?> right) {
        if (hasNull(left, right)) {
            return BooleanWrapper.get(false);
        }
        boolean res;
        if (left.isNull() && right.isNull()) {
            res = true;
        } else if (left.isNull() || right.isNull()) {
            res = false;
        } else {
            res = left.equals(right);
        }
        return BooleanWrapper.get(!res);
    }

    public static BaseWrapper<?> in(BaseWrapper<?> val, BaseWrapper<?> tmp) {
        if (hasNull(val)) {
            return BooleanWrapper.get(false);
        }
        ListWrapper list = (ListWrapper) tmp;
        boolean in = false;
        for (int i = 0; i < list.size(); ++i) {
            if (val.equals(list.get(i))) {
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
