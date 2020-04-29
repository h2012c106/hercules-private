package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class StatementGetter {
    private static final Log LOG = LogFactory.getLog(StatementGetter.class);

    private List<String> filterNullColumns(List<String> columnNameList, String columnMask) {
        List<String> tmpList = new ArrayList<>(columnNameList.size());
        for (int i = 0; i < columnNameList.size(); ++i) {
            if (columnMask.charAt(i) == '1') {
                tmpList.add(columnNameList.get(i));
            }
        }
        return tmpList;
    }

    public final String getExportSql(String tableName, List<String> columnNameList, String columnMask, int numRows) {
        columnNameList = filterNullColumns(columnNameList, columnMask);
        return innerGetExportSql(tableName, columnNameList, columnMask, numRows);
    }

    public final String getExportSql(String tableName, List<String> columnNameList, String columnMask, List<String> updateKeyList) {
        columnNameList = filterNullColumns(columnNameList, columnMask);
        return innerGetExportSql(tableName, columnNameList, columnMask, updateKeyList);
    }

    public final String getMigrateSql(String tableName, String stagingTableName, List<String> columnNameList) {
        return innerGetMigrateSql(tableName, stagingTableName, columnNameList);
    }

    abstract protected String innerGetExportSql(String tableName, List<String> columnNameList, String columnMask, int numRows);

    abstract protected String innerGetExportSql(String tableName, List<String> columnNameList, String columnMask, List<String> updateKeyList);

    abstract protected String innerGetMigrateSql(String tableName, String stagingTableName, List<String> columnNameList);
}
