package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.xlog.canal.CanalSerDe;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public class MysqlCanalEntryKvConverter extends KvConverter<Integer, ResultSet, CanalEntry.Column, CanalEntry.Column.Builder> {

    public MysqlCanalEntryKvConverter() {
        super(new MysqlCanalEntryDataTypeConverter(), new CanalMysqlWrapperGetterFactory(), new CanalMysqlWrapperSetterFactory());
    }

    @Override
    public byte[] generateValue(HerculesWritable value, GenericOptions options, Map<String, DataType> columnTypeMap, List<String> columnNameList) {
        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.INSERT);
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setEventType(CanalEntry.EventType.INSERT);
        headerBuilder.setSourceType(CanalEntry.Type.MYSQL);

        headerBuilder.setSchemaName(options.getString(CanalMysqlOutputOptionConf.SCHEMA_NAME, ""));
        headerBuilder.setTableName(options.getString(CanalMysqlOutputOptionConf.TABLE_NAME, ""));

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        String keyCol = options.getString(CanalMysqlOutputOptionConf.KEY, "");

        for (String columnName : columnNameList) {

            BaseWrapper wrapper = value.get(columnName);
            DataType type = columnTypeMap.get(columnName);
            if (type == null) {
                type = wrapper.getType();
            }

            CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder()
                    .setName(columnName)
                    .setSqlType((Integer) dataTypeConverter.convertElementType(type));
            try {
                getWrapperSetter(type).set(wrapper, columnBuilder, "", "", 0);
            } catch (Exception e) {
                e.printStackTrace();
            }
            columnBuilder.setIsKey(columnName.equals(keyCol)).setIsNull(false);
            rowDataBuilder.addAfterColumns(columnBuilder.build());
        }
        rowChangeBuilder.addRowDatas(rowDataBuilder.build());
        CanalEntry.Entry entry =  CanalEntry.Entry.newBuilder()
                .setHeader(headerBuilder.build())
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setStoreValue(rowChangeBuilder.build().toByteString())
                .build();
        try {
            return CanalSerDe.serialize(entry);
        } catch (SerDeException e) {
            return null;
        }
    }

    @Override
    public HerculesWritable generateHerculesWritable(byte[] data, GenericOptions options) throws IOException {

        CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(data);
        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND ||
                entry.getHeader().getSourceType() != CanalEntry.Type.MYSQL) {
            throw new RuntimeException("Entry type is not MYSQL.");
        }
        CanalEntry.RowChange rowChange;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("ERROR. Parse of event has an error , data:" + entry.toString(), e);
        }

        CanalEntry.EventType eventType = rowChange.getEventType();

        HerculesWritable record = new HerculesWritable(rowChange.getRowDatas(0).getAfterColumnsList().size());
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            if (eventType != CanalEntry.EventType.INSERT) {
                throw new RuntimeException("ERROR, entry event type should be INSERT.");
            }
            List<CanalEntry.Column> columns = rowData.getAfterColumnsList();

            for (CanalEntry.Column column : columns) {
                DataType type = dataTypeConverter.convertElementType(column.getSqlType());
                try {
                    record.put(column.getName(), getWrapperGetter(type).get(column, null, column.getName(), 0));
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
        return record;
    }
}
