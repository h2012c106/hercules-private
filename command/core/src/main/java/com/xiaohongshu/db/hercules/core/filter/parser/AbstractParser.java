package com.xiaohongshu.db.hercules.core.filter.parser;

import com.xiaohongshu.db.hercules.core.filter.expr.CombinationExpr;
import com.xiaohongshu.db.hercules.core.filter.expr.Expr;
import com.xiaohongshu.db.hercules.core.filter.expr.FunctionExpr;
import com.xiaohongshu.db.hercules.core.filter.function.FilterCoreFunction;
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

    private boolean canOptimize(Expr expr) {
        return expr instanceof CombinationExpr && ((CombinationExpr) expr).getType().isAnd();
    }

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

    /**
     * 优化，遇到OR子树就不用往下看，遇到AND子树就向下看后就向上合并，方便判断下推
     *
     * @param root
     */
    private void optimize(Expr root) {
        if (!canOptimize(root)) {
            return;
        }
        for (Expr child : root.getChildren()) {
            optimize(child);
        }
        List<Integer> removeSeqList = new LinkedList<>();
        List<Expr> addList = new LinkedList<>();
        for (int i = 0; i < root.getChildren().size(); ++i) {
            Expr child = root.getChildren().get(i);
            if (!canOptimize(child)) {
                continue;
            }
            removeSeqList.add(i);
            for (Expr grandChild : child.getChildren()) {
                grandChild.setParent(root);
                addList.add(grandChild);
            }
            child.getChildren().clear();
        }
        Collections.reverse(removeSeqList);
        for (int i : removeSeqList) {
            root.getChildren().remove(i).setParent(null);
        }
        root.getChildren().addAll(addList);
    }

    /**
     * 现在只使用幂等率简化，但是未来可以使用吸收率、同一律、零率继续简化，以及优先计算出常量计算值
     *
     * @param expr
     */
    private void simplify(Expr expr) {
        for (Expr child : expr.getChildren()) {
            simplify(child);
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
                if (functionChild.getMethod() == PUSHDOWN_MARK_METHOD) {
                    Expr grandChild = functionChild.getChildren().get(0);
                    grandChild.setForcePushdown();
                    // 把孙子拉成儿子
                    grandChild.setParent(root);
                    root.getChildren().set(i, grandChild);
                    child.getChildren().clear();
                    child.setParent(null);
                } else if (functionChild.getMethod() == NOT_PUSHDOWN_MARK_METHOD) {
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
        // 先把根结点转为AND
        res = checkRootAnd(res);
        optimize(res);
        LOG.info("Filter optimized as: " + res.toString());
        simplify(res);
        LOG.info("Filter simplified as: " + res.toString());
        dealWithPushdownMark(res);
        return res;
    }
}
