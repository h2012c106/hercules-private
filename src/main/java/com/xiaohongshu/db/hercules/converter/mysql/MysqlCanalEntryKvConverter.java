package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Map;

public class MysqlCanalEntryKvConverter extends KvConverter {

    @Override
    public String convertValue(BaseWrapper wrapper) {
        BaseDataType type = wrapper.getType().getBaseDataType();
        switch (type) {
            case BYTES:
                return new String(wrapper.asBytes(), StandardCharsets.ISO_8859_1);
            case BYTE:
                return String.valueOf(wrapper.asBigInteger().byteValueExact());
            case SHORT:
                return String.valueOf(wrapper.asBigInteger().shortValueExact());
            case INTEGER:
                return String.valueOf(wrapper.asBigInteger().intValueExact());
            case LONG:
                return String.valueOf(wrapper.asBigInteger().longValueExact());
            case FLOAT:
                return String.valueOf(OverflowUtils.numberToFloat(wrapper.asBigDecimal()));
            case DOUBLE:
                return String.valueOf(OverflowUtils.numberToDouble(wrapper.asBigDecimal()));
            case DECIMAL:
                return String.valueOf(wrapper.asBigDecimal());
            case BOOLEAN:
                return String.valueOf(wrapper.asBoolean());
            case STRING:
            case DATE:
            case DATETIME:
                return wrapper.asString();
            case TIME:
                return wrapper.asDate().toString();
            default:
                throw new RuntimeException("Unknown column type: " + type.getBaseDataType().name());
        }
    }

    @Override
    public int getColumnType(DataType type) {
        switch (type.getBaseDataType()) {
            case BYTES:
                return Types.BINARY;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case DECIMAL:
                return Types.DECIMAL;
            case BOOLEAN:
                return Types.BOOLEAN;
            case STRING:
                return Types.VARCHAR;
            case DATE:
                return Types.DATE;
            case DATETIME:
                return Types.TIMESTAMP;
            case TIME:
                return Types.TIME;
            default:
                throw new RuntimeException("Unknown column type: " + type.getBaseDataType().name());
        }
    }

    @Override
    public byte[] generateValue(HerculesWritable value, GenericOptions options) {
        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.INSERT);
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setEventType(CanalEntry.EventType.INSERT);
        headerBuilder.setSourceType(CanalEntry.Type.MYSQL);

        headerBuilder.setSchemaName(options.getString(CanalMysqlOptionConf.SCHEMA_NAME, ""));
        headerBuilder.setTableName(options.getString(CanalMysqlOptionConf.TABLE_NAME, ""));

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        String keyCol = options.getString(CanalMysqlOptionConf.KEY, "");

        for (Map.Entry<String, BaseWrapper> entry : value.entrySet()) {
            BaseWrapper wrapper = entry.getValue();
            DataType type = wrapper.getType();
            String columnName = entry.getKey();
            String columnValue = convertValue(wrapper);

            CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder()
                    .setName(columnName)
                    .setSqlType(getColumnType(type))
                    .setIsKey(columnName.equals(keyCol))
                    .setIsNull(columnValue == null);
            if (columnValue != null) {
                columnBuilder.setValue(columnValue);
            }
            CanalEntry.Column column = columnBuilder.build();
            rowDataBuilder.addAfterColumns(column);
        }
        rowChangeBuilder.addRowDatas(rowDataBuilder.build());
        return CanalEntry.Entry.newBuilder()
                .setHeader(headerBuilder.build())
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setStoreValue(rowChangeBuilder.build().toByteString())
                .build().toByteArray();
    }
}
