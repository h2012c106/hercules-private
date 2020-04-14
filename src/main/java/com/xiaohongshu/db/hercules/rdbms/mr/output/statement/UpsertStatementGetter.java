package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import java.util.Arrays;
import java.util.stream.Collectors;

public class UpsertStatementGetter extends StatementGetter {
    @Override
    public String getExportSql(String tableName, String[] columnNames, int numRows) {
        columnNames = filterNullColumns(columnNames);

        boolean first;
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT IGNORE INTO ");
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

        sb.append(") ON DUPLICATE KEY UPDATE ");

        first = true;
        for (String column : columnNames) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append("`").append(column).append("`").append(" = VALUES(`").append(column).append("`)");
        }

        sb.append(";");

        return sb.toString();
    }

    @Override
    public String getExportSql(String tableName, String[] columnNames, String[] updateKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMigrateSql(String tableName, String stagingTableName, String[] columnNames) {
        return String.format("INSERT INTO `%s` SELECT * FROM `%s` ON DUPLICATE KEY UPDATE %s",
                tableName,
                stagingTableName,
                Arrays.stream(columnNames)
                        .map(columnName
                                -> String.format("`%s` = VALUES(`%s`)", columnName, columnName))
                        .collect(Collectors.joining(", "))
        );
    }
}
