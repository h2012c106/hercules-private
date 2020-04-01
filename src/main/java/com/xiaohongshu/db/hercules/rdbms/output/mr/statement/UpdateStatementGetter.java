package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

public class UpdateStatementGetter extends StatementGetter {
    @Override
    public String getExportSql(String tableName, String[] columnNames, int numRows) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getExportSql(String tableName, String[] columnNames, String[] updateKeys) {
        columnNames = filterNullColumns(columnNames);

        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE `").append(tableName).append("` SET ");

        boolean first = true;
        for (String col : columnNames) {
            if (!first) {
                sb.append(", ");
            }

            sb.append("`").append(col).append("`");
            sb.append(" = ?");
            first = false;
        }

        sb.append(" WHERE ");
        first = true;
        for (String updateKey : updateKeys) {
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
    public String getMigrateSql(String tableName, String stagingTableName, String[] columnNames) {
        throw new UnsupportedOperationException();
    }
}
