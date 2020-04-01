package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

public interface StatementGetter {
    public String get(String tableName, String[] columnNames, int numRows);

    public String get(String tableName, String[] columnNames, String[] updateKeys);
}
