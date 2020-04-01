package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

public class UpdateStatementGetter implements StatementGetter {
    @Override
    public String get(String tableName, String[] columnNames, int numRows) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String get(String tableName, String[] columnNames, String[] updateKeys) {
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
}
