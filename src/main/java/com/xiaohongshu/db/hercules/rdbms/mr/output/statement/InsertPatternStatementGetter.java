package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

public abstract class InsertPatternStatementGetter extends StatementGetter {

    abstract protected String getMethod();

    @Override
    public String getExportSql(String tableName, String[] columnNames, int numRows) {
        columnNames = filterNullColumns(columnNames);

        boolean first;
        StringBuilder sb = new StringBuilder();
        sb.append(getMethod()).append(" INTO ");
        sb.append("`").append(tableName).append("`");
        sb.append("(");
        first = true;
        for (String column : columnNames) {
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
            for (int j = 0; j < columnNames.length; j++) {
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
    public String getExportSql(String tableName, String[] columnNames, String[] updateKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMigrateSql(String tableName, String stagingTableName, String[] columnNames) {
        return String.format("%s INTO `%s` SELECT * FROM `%s`", getMethod(), tableName, stagingTableName);
    }
}
