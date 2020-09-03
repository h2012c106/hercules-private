package com.xiaohongshu.db.hercules.core.filter.parser;

import com.xiaohongshu.db.hercules.core.filter.expr.Expr;

public interface Parser {
    public static final Parser INSTANCE = new DruidParser();

    public Expr parse(String str);
}
