package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class RDBMSUpdateRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSUpdateRecordWriter.class);

    private String[] updateKeys;
    private List<Integer> updateSourceColumnSeq;
    private String updateSql;

    private List<WrapperSetter<PreparedStatement>> updateKeyWrapperSetterList;

    public RDBMSUpdateRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSSchemaFetcher schemaFetcher)
            throws SQLException, ClassNotFoundException {
        super(context, tableName, exportType, schemaFetcher);

        updateKeys = options.getTargetOptions().getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null);
        JSONObject columnMap = options.getCommonOptions().getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject());

        // 生成update key对应的上游下标，且必须能在上游找到对应
        updateSourceColumnSeq = SchemaUtils.mapColumnSeq(sourceColumnList, Arrays.asList(updateKeys), columnMap);
        if (updateSourceColumnSeq.contains(null)) {
            throw new MapReduceException("The update key should be the subset of source data source columns.");
        }

        updateKeyWrapperSetterList = makeWrapperSetterList(schemaFetcher, Arrays.asList(updateKeys));

        // update都是一个语句一个语句更新的，每个prepared sql都一样
        updateSql = statementGetter.getExportSql(tableName, columnNames, updateKeys);

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
            for (int i = 0; i < updateKeys.length; ++i) {
                int sourceSeq = updateSourceColumnSeq.get(i);
                // meaningfulSeq得续上
                updateKeyWrapperSetterList.get(i).set(record.get(sourceSeq), preparedStatement, null, ++meaningfulSeq);
            }
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }
}
