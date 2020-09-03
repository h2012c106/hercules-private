package com.xiaohongshu.db.hercules.core.filter.expr;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class ListExpr extends AbstractExpr {

    private List<Expr> list;

    public ListExpr(List<Expr> list) {
        this.list = list;
        for (Expr expr : list) {
            expr.setParent(this);
        }
    }

    @Override
    public BaseWrapper<?> getResult(HerculesWritable row) {
        ListWrapper res = new ListWrapper(list.size());
        for (Expr expr : list) {
            res.add(expr.getResult(row));
        }
        return res;
    }

    @Override
    public List<Expr> getChildren() {
        return list;
    }

    @Override
    public String toString() {
        return "LIST<" + StringUtils.join(list, ", ") + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListExpr listExpr = (ListExpr) o;
        return Objects.equal(list, listExpr.list);
    }
}
