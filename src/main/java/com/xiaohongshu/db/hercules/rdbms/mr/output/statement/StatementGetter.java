package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import java.util.ArrayList;
import java.util.List;

public abstract class StatementGetter {
    protected String[] filterNullColumns(String[] columns, String columnMask) {
        List<String> tmpList = new ArrayList<>(columns.length);
        for (int i = 0; i < columns.length; ++i) {
            if (columnMask.charAt(i) == '1') {
                tmpList.add(columns[i]);
            }
        }
        return tmpList.toArray(new String[0]);
    }

    abstract public String getExportSql(String tableName, String[] columnNames, String columnMask, int numRows);

    abstract public String getExportSql(String tableName, String[] columnNames, String columnMask, String[] updateKeys);

    abstract public String getMigrateSql(String tableName, String stagingTableName, String[] columnNames);
}
