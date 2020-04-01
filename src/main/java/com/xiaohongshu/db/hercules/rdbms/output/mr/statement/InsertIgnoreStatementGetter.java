package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

public class InsertIgnoreStatementGetter extends InsertPatternStatementGetter {
    @Override
    protected String getMethod() {
        return "INSERT IGNORE";
    }
}
