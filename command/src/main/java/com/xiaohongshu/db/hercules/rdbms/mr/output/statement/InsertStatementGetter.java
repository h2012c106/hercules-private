package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

public class InsertStatementGetter extends InsertPatternStatementGetter {
    @Override
    protected String getMethod() {
        return "INSERT";
    }
}
