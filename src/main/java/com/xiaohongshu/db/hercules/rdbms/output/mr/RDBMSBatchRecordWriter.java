package com.xiaohongshu.db.hercules.rdbms.output.mr;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class RDBMSBatchRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSBatchRecordWriter.class);

    private String updateSql;

    public RDBMSBatchRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSSchemaFetcher schemaFetcher)
            throws SQLException, ClassNotFoundException {
        super(context, tableName, exportType, schemaFetcher);

        // batch每次只有一行问号，每个prepared sql都一样
        updateSql = statementGetter.getExportSql(tableName, columnNames, 1);

        LOG.info("Update sql is: " + updateSql);
    }

    @Override
    protected PreparedStatement getPreparedStatement(List<HerculesWritable> recordList, Connection connection)
            throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
        for (HerculesWritable record : recordList) {
            // 排去null的下标
            int meaningfulSeq = 0;
            for (int i = 0; i < columnNames.length; ++i) {
                String columnName = columnNames[i];

                if (columnName == null) {
                    continue;
                }

                // 源数据源中该列的下标，即HerculesWritable中的下标
                int sourceSeq = targetSourceColumnSeq.get(i);
                WrapperSetter<PreparedStatement> setter = wrapperSetterList.get(i);
                // meaningfulSeq + 1为prepared statement里问号的下标
                setter.set(record.get(sourceSeq), preparedStatement, null, ++meaningfulSeq);
            }
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }
}
