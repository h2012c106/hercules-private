package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import java.util.List;

public class UpdateStatementGetter extends StatementGetter {
    @Override
    public String innerGetExportSql(String tableName, List<String> columnNameList, String columnMask, int numRows) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String innerGetExportSql(String tableName, List<String> columnNameList, String columnMask, List<String> updateKeyList) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE `").append(tableName).append("` SET ");

        boolean first = true;
        for (String col : columnNameList) {
            if (!first) {
                sb.append(", ");
            }

            sb.append("`").append(col).append("`");
            sb.append(" = ?");
            first = false;
        }

        sb.append(" WHERE ");
        first = true;
        for (String updateKey : updateKeyList) {
            if (first) {
                first = false;
            } else {
                sb.append(" AND ");
            }
            sb.append("`").append(updateKey).append("`").append(" = ?");
        }
        sb.append(";");
        return sb.toString();
    }

    @Override
    public String innerGetMigrateSql(String tableName, String stagingTableName, List<String> columnNameList) {
        throw new UnsupportedOperationException();
    }
}
