package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class StatementGetter {
    private static final Log LOG = LogFactory.getLog(StatementGetter.class);

    protected List<String> filterNullColumns(List<String> columnNameList, String columnMask) {
        List<String> tmpList = new ArrayList<>(columnNameList.size());
        for (int i = 0; i < columnNameList.size(); ++i) {
            if (columnMask.charAt(i) == '1') {
                tmpList.add(columnNameList.get(i));
            }
        }
        return tmpList;
    }

    abstract public String getExportSql(String tableName, List<String> columnNameList, String columnMask, int numRows);

    abstract public String getExportSql(String tableName, List<String> columnNameList, String columnMask, List<String> updateKeyList);

    abstract public String getMigrateSql(String tableName, String stagingTableName, List<String> columnNameList);
}
