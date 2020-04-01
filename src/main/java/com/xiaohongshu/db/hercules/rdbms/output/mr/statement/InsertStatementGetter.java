package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

public class InsertStatementGetter extends InsertPatternStatementGetter {
    @Override
    protected String getMethod() {
        return "INSERT";
    }
}
