package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

public class ReplaceStatementGetter extends InsertPatternStatementGetter {
    @Override
    protected String getMethod() {
        return "REPLACE";
    }
}
