package com.xiaohongshu.db.hercules.nebula.mr.output;

import com.facebook.thrift.TException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.vesoft.nebula.client.graph.GraphClient;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.nebula.NebulaDataMode;
import com.xiaohongshu.db.hercules.nebula.WritingRow;
import com.xiaohongshu.db.hercules.nebula.datatype.VidCustomDataType;
import com.xiaohongshu.db.hercules.nebula.schema.NebulaUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

import static com.xiaohongshu.db.hercules.nebula.option.NebulaOptionsConf.MODE;
import static com.xiaohongshu.db.hercules.nebula.option.NebulaOptionsConf.TABLE;
import static com.xiaohongshu.db.hercules.nebula.option.NebulaOutputOptionsConf.BATCH_SIZE;

public class NebulaRecordWriter extends HerculesRecordWriter<WritingRow> {

    private static final Log LOG = LogFactory.getLog(NebulaRecordWriter.class);

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    @SchemaInfo
    private Schema schema;

    private String table;
    private NebulaDataMode mode;
    private int batchSize;
    private GraphClient client;
    private List<String> columnNameList;
    private String[] keyNameList;

    private final List<WritingRow> rowCache = new LinkedList<>();

    public NebulaRecordWriter(TaskAttemptContext context) {
        super(context);
    }

    @Override
    protected void innerAfterInject() {
        super.innerAfterInject();
        table = targetOptions.getString(TABLE, null);
        mode = NebulaDataMode.valueOfIgnoreCase(targetOptions.getString(MODE, null));
        batchSize = targetOptions.getInteger(BATCH_SIZE, null);
        keyNameList = mode.getGetVidNamesFunction().apply(targetOptions);
        columnNameList = new ArrayList<>(schema.getColumnNameList());
        columnNameList.removeAll(Arrays.asList(keyNameList));
        try {
            client = NebulaUtils.getConnection(targetOptions);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    private List<String> mergeBatchCautiously(List<WritingRow> rowBatch) {
        Multimap<Set<String>, WritingRow> groupedRowBatch = ArrayListMultimap.create();
        for (WritingRow row : rowBatch) {
            groupedRowBatch.put(row.getValueMap().keySet(), row);
        }
        List<String> res = new LinkedList<>();
        for (Set<String> columnSet : groupedRowBatch.keySet()) {
            Collection<WritingRow> sameColumnRowGroup = groupedRowBatch.get(columnSet);
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT ")
                    .append(mode.getInsertName())
                    .append(" ")
                    .append(table)
                    .append("(")
                    .append(StringUtils.join(columnSet, ", "))
                    .append(") VALUES ");
            List<String> tmpList = new LinkedList<>();
            for (WritingRow row : sameColumnRowGroup) {
                String head = mode.getGetInsertRowHeadFunction().apply(targetOptions, row.getKeyMap());
                List<String> tmpTmpList = new LinkedList<>();
                for (String key : columnSet) {
                    tmpTmpList.add(row.getValueMap().get(key));
                }
                tmpList.add(head + ":(" + StringUtils.join(tmpTmpList, ", ") + ")");
            }
            sb.append(StringUtils.join(tmpList, ", ")).append(";");
            res.add(sb.toString());
        }
        return res;
    }

    private List<String> mergeBatch(List<WritingRow> rowBatch) {
        if (rowBatch.size() == 0) {
            return Collections.emptyList();
        } else {
            Set<String> columnSet = rowBatch.get(0).getValueMap().keySet();
            List<String> res = new LinkedList<>();
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT ")
                    .append(mode.getInsertName())
                    .append(" ")
                    .append(table)
                    .append("(")
                    .append(StringUtils.join(columnSet, ", "))
                    .append(") VALUES ");
            List<String> tmpList = new LinkedList<>();
            for (WritingRow row : rowBatch) {
                String head = mode.getGetInsertRowHeadFunction().apply(targetOptions, row.getKeyMap());
                List<String> tmpTmpList = new LinkedList<>();
                for (String key : columnSet) {
                    tmpTmpList.add(row.getValueMap().get(key));
                }
                tmpList.add(head + ":(" + StringUtils.join(tmpTmpList, ", ") + ")");
            }
            sb.append(StringUtils.join(tmpList, ", ")).append(";");
            res.add(sb.toString());
            return res;
        }
    }

    private WritingRow convert(HerculesWritable value) throws Exception {
        WritingRow row = new WritingRow();
        for (String columnName : columnNameList) {
            DataType dataType = schema.getColumnTypeMap().get(columnName);
            getWrapperSetter(dataType).set(value.get(columnName), row, null, columnName, -1);
        }
        for (String keyColumnName : keyNameList) {
            getWrapperSetter(VidCustomDataType.INSTANCE).set(value.get(keyColumnName), row, null, keyColumnName, -1);
        }
        return row;
    }

    private void execute(List<WritingRow> rowCache) throws IOException {
        List<String> sqlList = mergeBatch(rowCache);
        for (String sql : sqlList) {
            NebulaUtils.executeUpdate(client, sql);
        }
    }

    @Override
    protected void innerWrite(HerculesWritable value) throws IOException, InterruptedException {
        try {
            rowCache.add(convert(value));
        } catch (Exception e) {
            throw new IOException(e);
        }
        if (rowCache.size() >= batchSize) {
            execute(rowCache);
            rowCache.clear();
        }
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        if (rowCache.size() > 0) {
            execute(rowCache);
        }
        try {
            client.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return WritableUtils.FilterUnexistOption.DEFAULT_NULL_VALUE;
    }
}
