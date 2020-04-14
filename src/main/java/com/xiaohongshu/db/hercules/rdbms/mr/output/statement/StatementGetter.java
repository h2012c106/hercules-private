package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import java.util.Arrays;
import java.util.Objects;

public abstract class StatementGetter {
    protected String[] filterNullColumns(String[] columns) {
        return Arrays.stream(columns).filter(Objects::nonNull).toArray(String[]::new);
    }

    abstract public String getExportSql(String tableName, String[] columnNames, int numRows);

    abstract public String getExportSql(String tableName, String[] columnNames, String[] updateKeys);

    abstract public String getMigrateSql(String tableName, String stagingTableName, String[] columnNames);
}
