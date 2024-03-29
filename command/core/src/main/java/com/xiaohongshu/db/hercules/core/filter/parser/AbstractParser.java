package com.xiaohongshu.db.hercules.core.filter.parser;

import com.xiaohongshu.db.hercules.core.filter.expr.*;
import com.xiaohongshu.db.hercules.core.filter.function.FilterCoreFunction;
import com.xiaohongshu.db.hercules.core.filter.function.annotation.IgnoreOptimize;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractParser implements Parser {

    private static final Log LOG = LogFactory.getLog(DruidParser.class);

    private static final String PUSHDOWN_MARK_METHOD_NAME = "pushdown";
    private static final Method PUSHDOWN_MARK_METHOD = MethodUtils.getAccessibleMethod(
            FilterCoreFunction.class,
            PUSHDOWN_MARK_METHOD_NAME,
            BaseWrapper.class
    );
    private static final String NOT_PUSHDOWN_MARK_METHOD_NAME = "notPushdown";
    private static final Method NOT_PUSHDOWN_MARK_METHOD = MethodUtils.getAccessibleMethod(
            FilterCoreFunction.class,
            NOT_PUSHDOWN_MARK_METHOD_NAME,
            BaseWrapper.class
    );

    /**
     * 保证根节点是AND
     *
     * @param expr
     * @return
     */
    private Expr checkRootAnd(Expr expr) {
        if (expr instanceof CombinationExpr && ((CombinationExpr) expr).getType().isAnd()) {
            return expr;
        } else {
            return new CombinationExpr(CombinationExpr.CombinationType.AND, expr);
        }
    }

    private boolean canPrecalc(Expr root) {
        if (root instanceof ColumnExpr) {
            return false;
        } else if (root instanceof FunctionExpr) {
            FunctionExpr functionExpr = (FunctionExpr) root;
            Method method = functionExpr.getMethod();
            if (method.getAnnotation(IgnoreOptimize.class) != null) {
                return false;
            }
        }
        for (Expr child : root.getChildren()) {
            if (!canPrecalc(child)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 进行预计算简化
     *
     * @param root
     */
    private Expr precalc(Expr root) {
        if (root instanceof ValueExpr) {
            return root;
        } else if (canPrecalc(root)) {
            // 如果自己能直接预计算，那么说明子树均可，直接算就完事了
            // TODO 这里在递归规程中，子树的canPrecalc函数会被多次调用重复计算，其实可以搞一个Map之类的存起来，不过懒得弄了，反正是一次性函数
            Expr res = new ValueExpr(root.getResult(null));
            res.setParent(root.getParent());
            root.setParent(null);
            return res;
        } else {
            for (int i = 0; i < root.getChildren().size(); ++i) {
                Expr child = root.getChildren().get(i);
                root.getChildren().set(i, precalc(child));
            }
            return root;
        }
    }

    private boolean isAndExpr(Expr expr) {
        return expr instanceof CombinationExpr && ((CombinationExpr) expr).getType().isAnd();
    }

    /**
     * 优化，遇到OR子树就不用往下看，遇到AND子树就向下看后就向上合并，方便判断下推
     *
     * @param root
     */
    private void mergeAndTree(Expr root) {
        if (!isAndExpr(root)) {
            return;
        }
        for (Expr child : root.getChildren()) {
            mergeAndTree(child);
        }
        List<Expr> addList = new LinkedList<>();
        for (int i = 0; i < root.getChildren().size(); ++i) {
            Expr child = root.getChildren().get(i);
            if (!isAndExpr(child)) {
                addList.add(child);
            } else {
                for (Expr grandChild : child.getChildren()) {
                    grandChild.setParent(root);
                    addList.add(grandChild);
                }
                child.getChildren().clear();
                child.setParent(null);
            }
        }
        root.getChildren().clear();
        root.getChildren().addAll(addList);
    }

    /**
     * 现在只使用幂等率简化，但是未来可以使用吸收率、同一律、零率继续简化，以及优先计算出常量计算值
     *
     * @param expr
     */
    private void idempotent(Expr expr) {
        for (Expr child : expr.getChildren()) {
            idempotent(child);
        }
        if (expr instanceof CombinationExpr) {
            List<Integer> removeSeqList = new LinkedList<>();
            for (int i = 0; i < expr.getChildren().size(); ++i) {
                for (int j = 0; j < i; ++j) {
                    if (expr.getChildren().get(i).equals(expr.getChildren().get(j))) {
                        removeSeqList.add(i);
                        break;
                    }
                }
            }
            Collections.reverse(removeSeqList);
            for (int i : removeSeqList) {
                expr.getChildren().remove(i).setParent(null);
            }
        }
    }

    /**
     * 只处理根节点下第一层条件，再往下看没有任何意义
     *
     * @param root
     */
    private void dealWithPushdownMark(Expr root) {
        for (int i = 0; i < root.getChildren().size(); ++i) {
            Expr child = root.getChildren().get(i);
            if (child instanceof FunctionExpr) {
                FunctionExpr functionChild = (FunctionExpr) child;
                if (functionChild.getMethod().equals(PUSHDOWN_MARK_METHOD)) {
                    Expr grandChild = functionChild.getChildren().get(0);
                    grandChild.setForcePushdown();
                    // 把孙子拉成儿子
                    grandChild.setParent(root);
                    root.getChildren().set(i, grandChild);
                    child.getChildren().clear();
                    child.setParent(null);
                } else if (functionChild.getMethod().equals(NOT_PUSHDOWN_MARK_METHOD)) {
                    Expr grandChild = functionChild.getChildren().get(0);
                    grandChild.setForceNotPushdown();
                    // 把孙子拉成儿子
                    grandChild.setParent(root);
                    root.getChildren().set(i, grandChild);
                    child.getChildren().clear();
                    child.setParent(null);
                }
            }
        }
    }

    abstract protected Expr innerParse(String str);

    @Override
    public final Expr parse(String str) {
        Expr res = innerParse(str);
        LOG.info("Filter parsed as: " + res.toString());
        res = precalc(res);
        res = checkRootAnd(res);
        mergeAndTree(res);
        idempotent(res);
        dealWithPushdownMark(res);
        LOG.info("Filter optimized as: " + res.toString());
        return res;
    }
}
