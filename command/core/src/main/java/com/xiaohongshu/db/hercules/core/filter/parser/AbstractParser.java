package com.xiaohongshu.db.hercules.core.filter.parser;

import com.xiaohongshu.db.hercules.core.filter.expr.CombinationExpr;
import com.xiaohongshu.db.hercules.core.filter.expr.Expr;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractParser implements Parser {

    private static final Log LOG = LogFactory.getLog(DruidParser.class);

    private boolean canOptimize(Expr expr) {
        return expr instanceof CombinationExpr && ((CombinationExpr) expr).getType().isAnd();
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

    abstract protected Expr innerParse(String str);

    @Override
    public final Expr parse(String str) {
        Expr res = innerParse(str);
        LOG.info("Filter parsed as: " + res.toString());
        optimize(res);
        LOG.info("Filter optimized as: " + res.toString());
        simplify(res);
        LOG.info("Filter simplified as: " + res.toString());
        return res;
    }
}
