package com.xiaohongshu.db.hercules.core.filter.expr;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

import java.util.List;

public interface Expr {

    public BaseWrapper<?> getResult(HerculesWritable row);

    public void setParent(Expr parent);

    public Expr getParent();

    public List<Expr> getChildren();
}
