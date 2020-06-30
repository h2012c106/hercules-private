package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import java.util.List;

public abstract class InsertPatternStatementGetter extends StatementGetter {

    abstract protected String getMethod();

    @Override
    public String innerGetExportSql(String tableName, List<String> columnNameList, String columnMask, int numRows) {
        boolean first;
        StringBuilder sb = new StringBuilder();
        sb.append(getMethod()).append(" INTO ");
        sb.append("`").append(tableName).append("`");
        sb.append("(");
        first = true;
        for (String column : columnNameList) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append("`").append(column).append("`");
        }

        sb.append(") VALUES (");
        for (int i = 0; i < numRows; i++) {
            if (i > 0) {
                sb.append("), (");
            }
            for (int j = 0; j < columnNameList.size(); j++) {
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append("?");
            }
        }

        sb.append(");");

        return sb.toString();
    }

    @Override
    public String innerGetExportSql(String tableName, List<String> columnNameList, String columnMask, List<String> updateKeyList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String innerGetMigrateSql(String tableName, String stagingTableName, List<String> columnNameList) {
        return String.format("%s INTO `%s` SELECT * FROM `%s`", getMethod(), tableName, stagingTableName);
    }
}
