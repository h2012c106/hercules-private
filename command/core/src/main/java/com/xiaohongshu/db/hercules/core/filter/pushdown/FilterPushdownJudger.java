package com.xiaohongshu.db.hercules.core.filter.pushdown;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.filter.expr.*;
import com.xiaohongshu.db.hercules.core.utils.entity.StingyMap;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.function.Function;

/**
 * 查询介质，如jdbc为string，mongo为document
 *
 * @param <T>
 */
public abstract class FilterPushdownJudger<T> {

    private static final Log LOG = LogFactory.getLog(FilterPushdownJudger.class);

    private Map<Class<? extends Expr>, Function<Expr, T>> convertMap;
    private List<FilterFunctionChecker> pushdownableMethodList;

    public FilterPushdownJudger() {
        this.convertMap = getConvertMap();
        this.convertMap = this.convertMap == null
                ? Collections.emptyMap()
                : this.convertMap;
        this.convertMap = new StingyMap<>(this.convertMap);
        this.pushdownableMethodList = getPushdownableMethodList();
        this.pushdownableMethodList = this.pushdownableMethodList == null
                ? Collections.emptyList()
                : this.pushdownableMethodList;
    }

    abstract protected Function<Expr, T> getColumnExprFunction();

    abstract protected Function<Expr, T> getCombinationExprFunction();

    abstract protected Function<Expr, T> getFunctionExprFunction();

    abstract protected Function<Expr, T> getListExprFunction();

    abstract protected Function<Expr, T> getValueExprFunction();

    private void setConvertMap(Class<? extends Expr> clazz, Map<Class<? extends Expr>, Function<Expr, T>> map) {
        try {
            Function<Expr, T> function = (Function<Expr, T>) MethodUtils.invokeMethod(this, true, "get" + clazz.getSimpleName() + "Function");
            if (function == null) {
                throw new UnsupportedOperationException();
            } else {
                map.put(clazz, function);
            }
        } catch (Exception e) {
            LOG.warn(String.format("Unsupported pushdown filter class: %s, exception: %s.", clazz.getCanonicalName(), e.getMessage()));
        }
    }

    /**
     * 为每个Expr提供转查询介质的方法，若不提供则默认不支持
     *
     * @return
     */
    private Map<Class<? extends Expr>, Function<Expr, T>> getConvertMap() {
        Map<Class<? extends Expr>, Function<Expr, T>> res = new HashMap<>();
        setConvertMap(ColumnExpr.class, res);
        setConvertMap(CombinationExpr.class, res);
        setConvertMap(FunctionExpr.class, res);
        setConvertMap(ListExpr.class, res);
        setConvertMap(ValueExpr.class, res);
        return res;
    }

    protected T convert(Expr expr) {
        return convertMap.get(expr.getClass()).apply(expr);
    }

    /**
     * 是否支持列名自成一查询条件而无相关逻辑判断符，如mysql'select * from tb where col;'是合法的
     *
     * @return
     */
    protected boolean canColumnSelfAsCondition() {
        return false;
    }

    /**
     * 给出哪些函数可以走索引，如gt
     *
     * @return
     */
    protected List<FilterFunctionChecker> getPushdownableMethodList() {
        return Lists.newArrayList(
                new FilterFunctionChecker("gt", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }),
                new FilterFunctionChecker("gte", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }),
                new FilterFunctionChecker("lt", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }),
                new FilterFunctionChecker("lte", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }),
                new FilterFunctionChecker("eq", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }),
                new FilterFunctionChecker("neq", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }),
                new FilterFunctionChecker("in", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValueList(exprs[1]);
                    }
                })
        );
    }

    protected boolean isColumn(Expr expr) {
        return expr instanceof ColumnExpr;
    }

    protected boolean isValue(Expr expr) {
        return expr instanceof ValueExpr;
    }

    protected boolean isFunction(Expr expr) {
        return expr instanceof FunctionExpr;
    }

    /**
     * list中若全是value，也算value
     */
    protected boolean isValueList(Expr expr) {
        if (expr instanceof ListExpr) {
            for (Expr child : expr.getChildren()) {
                if (!isValue(child)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private Expr removeChild(Expr parent, int seq) {
        parent.getChildren().get(seq).setParent(null);
        return parent.getChildren().remove(seq);
    }

    /**
     * 只看根结点和下一层的子节点，再往下不找and了，这个工作会在parser中做掉
     */
    public final T pushdown(Expr root) {
        // 只看and，其他没必要看
        if (root instanceof CombinationExpr && ((CombinationExpr) root).getType().isAnd()) {
            List<Integer> canPushdownSeq = new LinkedList<>();
            for (int i = 0; i < root.getChildren().size(); ++i) {
                Expr child = root.getChildren().get(i);
                if (canColumnSelfAsCondition() && isColumn(child)) {
                    canPushdownSeq.add(i);
                } else if (isFunction(child)) {
                    FunctionExpr functionChild = (FunctionExpr) child;
                    for (FilterFunctionChecker checker : pushdownableMethodList) {
                        if (checker.canPushdown(functionChild)) {
                            canPushdownSeq.add(i);
                            break;
                        }
                    }
                }
            }
            List<Expr> pushdownedExprList = new LinkedList<>();
            Collections.reverse(canPushdownSeq);
            for (int seq : canPushdownSeq) {
                pushdownedExprList.add(removeChild(root, seq));
            }
            CombinationExpr combinationExpr
                    = new CombinationExpr(CombinationExpr.CombinationType.AND, pushdownedExprList.toArray(new Expr[0]));
            return convert(combinationExpr);
        } else {
            return null;
        }
    }

    protected static class FilterFunctionChecker {
        private final String methodName;
        private final int paramNum;
        private final Function<Expr[], Boolean> checker;

        public FilterFunctionChecker(String methodName, int paramNum) {
            this(methodName, paramNum, new Function<Expr[], Boolean>() {
                /**
                 * 此处exprs的长度一定是paramNum，大可放心
                 * @param exprs
                 * @return
                 */
                @Override
                public Boolean apply(Expr... exprs) {
                    return true;
                }
            });
        }

        public FilterFunctionChecker(String methodName, int paramNum, Function<Expr[], Boolean> checker) {
            this.methodName = methodName;
            this.paramNum = paramNum;
            this.checker = checker;
        }

        public boolean canPushdown(FunctionExpr expr) {
            if (!expr.getMethod().getName().equals(methodName) || expr.getChildren().size() != paramNum) {
                return false;
            }
            return checker.apply(expr.getChildren().toArray(new Expr[0]));
        }

    }

}
