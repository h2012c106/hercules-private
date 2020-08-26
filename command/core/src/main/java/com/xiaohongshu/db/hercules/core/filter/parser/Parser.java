package com.xiaohongshu.db.hercules.core.filter.parser;

import com.xiaohongshu.db.hercules.core.filter.expr.Expr;

public interface Parser {
    public Expr parse(String str);
}
