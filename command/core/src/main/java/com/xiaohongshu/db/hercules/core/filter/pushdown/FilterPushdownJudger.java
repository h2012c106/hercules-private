package com.xiaohongshu.db.hercules.core.filter.pushdown;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.filter.expr.*;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.core.utils.entity.StingyMap;
import lombok.NonNull;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 查询介质，如jdbc为string，mongo为document
 *
 * @param <T>
 */
public abstract class FilterPushdownJudger<T> {

    private static final Log LOG = LogFactory.getLog(FilterPushdownJudger.class);

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

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
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                isColumn(exprs[0])
                                        ? ((ColumnExpr) exprs[0]).getColumnName()
                                        : ((ColumnExpr) exprs[1]).getColumnName()
                        );
                    }
                }),
                new FilterFunctionChecker("gte", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                isColumn(exprs[0])
                                        ? ((ColumnExpr) exprs[0]).getColumnName()
                                        : ((ColumnExpr) exprs[1]).getColumnName()
                        );
                    }
                }),
                new FilterFunctionChecker("lt", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                isColumn(exprs[0])
                                        ? ((ColumnExpr) exprs[0]).getColumnName()
                                        : ((ColumnExpr) exprs[1]).getColumnName()
                        );
                    }
                }),
                new FilterFunctionChecker("lte", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                isColumn(exprs[0])
                                        ? ((ColumnExpr) exprs[0]).getColumnName()
                                        : ((ColumnExpr) exprs[1]).getColumnName()
                        );
                    }
                }),
                new FilterFunctionChecker("eq", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                isColumn(exprs[0])
                                        ? ((ColumnExpr) exprs[0]).getColumnName()
                                        : ((ColumnExpr) exprs[1]).getColumnName()
                        );
                    }
                }),
                new FilterFunctionChecker("neq", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValue(exprs[1]) || isValue(exprs[0]) && isColumn(exprs[1]);
                    }
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                isColumn(exprs[0])
                                        ? ((ColumnExpr) exprs[0]).getColumnName()
                                        : ((ColumnExpr) exprs[1]).getColumnName()
                        );
                    }
                }),
                new FilterFunctionChecker("in", 2, new Function<Expr[], Boolean>() {
                    @Override
                    public Boolean apply(Expr... exprs) {
                        return isColumn(exprs[0]) && isValueList(exprs[1]);
                    }
                }, new Function<Expr[], List<String>>() {
                    @Override
                    public List<String> apply(Expr[] exprs) {
                        // 至少一个参数是列，不然根据上面的check不会过来
                        return Collections.singletonList(
                                ((ColumnExpr) exprs[0]).getColumnName()
                        );
                    }
                })
        );
    }

    protected final boolean isColumn(Expr expr) {
        return expr instanceof ColumnExpr;
    }

    protected final boolean isValue(Expr expr) {
        return expr instanceof ValueExpr;
    }

    protected final boolean isFunction(Expr expr) {
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
    public final T pushdown(@NonNull Expr root) {
        // 只看and，or没必要看
        if (root instanceof CombinationExpr && ((CombinationExpr) root).getType().isAnd()) {
            Map<String, Set<Integer>> mayPushdownColumnExprSeqMap = new HashMap<>();
            List<Integer> forcePushdownList = new LinkedList<>();
            for (int i = 0; i < root.getChildren().size(); ++i) {
                Expr child = root.getChildren().get(i);
                // 若强制不下推，这个条件看都不用看
                if (child.isForceNotPushdown()) {
                    continue;
                }
                // 若强制下推，也不用看条件，直接塞
                if (child.isForcePushdown()) {
                    forcePushdownList.add(i);
                }
                if (canColumnSelfAsCondition() && isColumn(child)) {
                    mayPushdownColumnExprSeqMap.computeIfAbsent(((ColumnExpr) child).getColumnName(), key -> new HashSet<>()).add(i);
                } else if (isFunction(child)) {
                    FunctionExpr functionChild = (FunctionExpr) child;
                    // 逻辑是这样的：
                    // 1. 首先某函数必须得具备有可走索引的能力，如lt、gt。
                    // 2. 其次函数的参数也得符合一定条件（数据源能指定），如a>0没问题，但是len(a)>0就不能走索引，故需要对参数类型也做检查。
                    // 3. 最后就算函数及函数当前用法保证能走索引，还得看列上是否有索引，这是下面做的事，这里先把列名记一下，考虑到存在复合索引，故需收集完统一判断。
                    for (FilterFunctionChecker checker : pushdownableMethodList) {
                        if (checker.canPushdown(functionChild)) {
                            for (String functionColumnParam : checker.getTheoreticalIndexColumnList(functionChild)) {
                                mayPushdownColumnExprSeqMap.computeIfAbsent(functionColumnParam, key -> new HashSet<>()).add(i);
                            }
                            break;
                        }
                    }
                }
            }

            // 判断每一对index group是否是当前filter中可以走索引的列的集合的子集，如果是，那么这一列相关的expr可以全部下推
            Map<String, Set<Integer>> canPushdownColumnExprSeqMap = new HashMap<>();
            for (Set<String> indexGroup : schema.getIndexGroupList()) {
                if (CollectionUtils.isSubCollection(indexGroup, mayPushdownColumnExprSeqMap.keySet())) {
                    for (String column : indexGroup) {
                        canPushdownColumnExprSeqMap.putIfAbsent(column, mayPushdownColumnExprSeqMap.get(column));
                    }
                }
            }

            Set<Integer> canPushdownSeqSet = canPushdownColumnExprSeqMap.values()
                    .stream()
                    .reduce(new HashSet<>(), new BinaryOperator<Set<Integer>>() {
                        @Override
                        public Set<Integer> apply(Set<Integer> integers, Set<Integer> integers2) {
                            integers.addAll(integers2);
                            return integers;
                        }
                    });
            canPushdownSeqSet.addAll(forcePushdownList);
            List<Integer> canPushdownSeqList = canPushdownSeqSet.stream()
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());

            List<Expr> pushdownedExprList = new LinkedList<>();
            for (int seq : canPushdownSeqList) {
                // 若想在hercules内再走一遍全部的filter确保一下，可以使用如下代码
                pushdownedExprList.add(root.getChildren().get(seq));
                // 此处会把下推条件从hercules的filter内去掉
                // pushdownedExprList.add(removeChild(root, seq));
            }
            if (pushdownedExprList.size() == 0) {
                return null;
            } else {
                CombinationExpr combinationExpr
                        = new CombinationExpr(CombinationExpr.CombinationType.AND, Lists.reverse(pushdownedExprList).toArray(new Expr[0]));
                return convert(combinationExpr);
            }
        } else {
            return null;
        }
    }

    protected static class FilterFunctionChecker {
        private final String methodName;
        private final int paramNum;
        /**
         * 检查某个function的参数是否满足下推条件，如虽然lt属于可下推函数，但是-1<0这种条件就没必要下推
         */
        private final Function<Expr[], Boolean> checker;
        /**
         * 给出一个可下推FunctionExpr内的理论上能走索引的列的名称，用于判断列上是否有索引
         */
        private final Function<Expr[], List<String>> theoreticalIndexColumnGetter;

        public FilterFunctionChecker(String methodName, int paramNum,
                                     Function<Expr[], Boolean> checker,
                                     Function<Expr[], List<String>> theoreticalIndexColumnGetter) {
            this.methodName = methodName;
            this.paramNum = paramNum;
            this.checker = checker;
            this.theoreticalIndexColumnGetter = theoreticalIndexColumnGetter;
        }

        public boolean canPushdown(FunctionExpr expr) {
            if (!expr.getMethod().getName().equals(methodName) || expr.getChildren().size() != paramNum) {
                return false;
            }
            return checker.apply(expr.getChildren().toArray(new Expr[0]));
        }

        public List<String> getTheoreticalIndexColumnList(FunctionExpr expr) {
            return theoreticalIndexColumnGetter.apply(expr.getChildren().toArray(new Expr[0]));
        }

    }

}
