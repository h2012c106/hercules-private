package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

public class UpsertStatementGetter implements StatementGetter {
    @Override
    public String get(String tableName, String[] columnNames, int numRows) {
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
    public String get(String tableName, String[] columnNames, String[] updateKeys) {
        throw new UnsupportedOperationException();
    }
}
