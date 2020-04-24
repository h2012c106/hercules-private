package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import java.util.List;
import java.util.stream.Collectors;

public class UpsertStatementGetter extends StatementGetter {
    @Override
    public String getExportSql(String tableName, List<String> columnNameList, String columnMask, int numRows) {
        columnNameList = filterNullColumns(columnNameList, columnMask);

        boolean first;
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT IGNORE INTO ");
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

        sb.append(") ON DUPLICATE KEY UPDATE ");

        first = true;
        for (String column : columnNameList) {
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
    public String getExportSql(String tableName, List<String> columnNameList, String columnMask, List<String> updateKeyList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMigrateSql(String tableName, String stagingTableName, List<String> columnNameList) {
        return String.format("INSERT INTO `%s` SELECT * FROM `%s` ON DUPLICATE KEY UPDATE %s",
                tableName,
                stagingTableName,
                columnNameList.stream()
                        .map(columnName
                                -> String.format("`%s` = VALUES(`%s`)", columnName, columnName))
                        .collect(Collectors.joining(", "))
        );
    }
}
