package com.xiaohongshu.db.hercules.core.filter.expr;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BooleanWrapper;
import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CombinationExpr extends AbstractExpr {

    private List<Expr> conditionList;
    private CombinationType type;

    public CombinationExpr(@NonNull CombinationType type, Expr... conditions) {
        this.type = type;
        for (Expr condition : conditions) {
            condition.setParent(this);
        }
        conditionList = new LinkedList<>(Arrays.asList(conditions));
    }

    @Override
    public BooleanWrapper getResult(HerculesWritable row) {
        if (CollectionUtils.isEmpty(conditionList)) {
            return (BooleanWrapper) BooleanWrapper.get(true);
        } else {
            return type.getCombinationFunction().combine(row, conditionList.toArray(new Expr[0]));
        }
    }

    @Override
    public List<Expr> getChildren() {
        return conditionList;
    }

    public CombinationType getType() {
        return type;
    }

    @Override
    public String toString() {
        return type.name() + "<" + StringUtils.join(conditionList, ", ") + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CombinationExpr that = (CombinationExpr) o;
        return Objects.equal(conditionList, that.conditionList) &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(conditionList, type);
    }

    public enum CombinationType {
        /**
         * 与
         */
        AND(new CombinationFunction() {
            @Override
            public BooleanWrapper combine(HerculesWritable row, Expr... conditions) {
                // 此处可强转，因为&&操作符不可能计算出null
                return (BooleanWrapper) BooleanWrapper.get(BooleanUtils.and(Arrays.stream(conditions).map(condition -> condition.getResult(row).asBoolean()).toArray(Boolean[]::new)));
            }
        }),
        /**
         * 或
         */
        OR(new CombinationFunction() {
            @Override
            public BooleanWrapper combine(HerculesWritable row, Expr... conditions) {
                // 此处可强转，因为||操作符不可能计算出null
                return (BooleanWrapper) BooleanWrapper.get(BooleanUtils.or(Arrays.stream(conditions).map(condition -> condition.getResult(row).asBoolean()).toArray(Boolean[]::new)));
            }
        });

        private final CombinationFunction combinationFunction;

        CombinationType(CombinationFunction combinationFunction) {
            this.combinationFunction = combinationFunction;
        }

        public CombinationFunction getCombinationFunction() {
            return combinationFunction;
        }

        public boolean isAnd() {
            return this == AND;
        }

        public boolean isOr() {
            return this == OR;
        }
    }

    private interface CombinationFunction {
        public BooleanWrapper combine(HerculesWritable row, Expr... conditions);
    }
}
