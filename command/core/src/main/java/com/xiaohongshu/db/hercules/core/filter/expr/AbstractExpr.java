package com.xiaohongshu.db.hercules.core.filter.expr;

import java.util.Collections;
import java.util.List;

public abstract class AbstractExpr implements Expr {

    private Expr parent;

    private boolean forcePushdown = false;
    private boolean forceNotPushdown = false;

    @Override
    public List<Expr> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public final void setParent(Expr parent) {
        this.parent = parent;
    }

    @Override
    public Expr getParent() {
        return parent;
    }

    @Override
    abstract public String toString();

    @Override
    abstract public boolean equals(Object obj);

    @Override
    public boolean isForcePushdown() {
        return forcePushdown;
    }

    @Override
    public void setForcePushdown() {
        this.forcePushdown = true;
    }

    @Override
    public boolean isForceNotPushdown() {
        return forceNotPushdown;
    }

    @Override
    public void setForceNotPushdown() {
        this.forceNotPushdown = true;
    }
}
