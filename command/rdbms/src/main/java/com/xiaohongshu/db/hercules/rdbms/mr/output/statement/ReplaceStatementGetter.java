package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

public class ReplaceStatementGetter extends InsertPatternStatementGetter {
    @Override
    protected String getMethod() {
        return "REPLACE";
    }
}
