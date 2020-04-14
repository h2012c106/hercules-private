package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

public class InsertIgnoreStatementGetter extends InsertPatternStatementGetter {
    @Override
    protected String getMethod() {
        return "INSERT IGNORE";
    }
}
