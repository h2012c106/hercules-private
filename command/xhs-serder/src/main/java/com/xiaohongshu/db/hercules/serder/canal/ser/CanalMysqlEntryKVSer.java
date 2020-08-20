package com.xiaohongshu.db.hercules.serder.canal.ser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.serder.canal.CanalMysqlOutputOptionConf;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SerDerAssembly;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CanalMysqlEntryKVSer extends KVSer<CanalEntry.Column.Builder> {

    @Options(type = OptionsType.SER)
    private GenericOptions options;

    /**
     * 由于需要填key信息，且schema中key信息不作抄袭，故需直接拿上游schema，有一丢丢破坏了上下游完全解耦原则不过还好
     */
    @SchemaInfo
    private Schema schema;

    @SerDerAssembly
    private RDBMSDataTypeConverter dataTypeConverter;

    public CanalMysqlEntryKVSer() {
        super(new CanalMysqlWrapperSetterFactory());
    }

    private boolean isKey(String columnName, List<Set<String>> ukGroupList) {
        for (Set<String> ukGroup : ukGroupList) {
            for (String key : ukGroup) {
                if (StringUtils.equalsIgnoreCase(key, columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    abstract protected byte[] serializeCanalEntry(CanalEntry.Entry entry);

    @Override
    protected BaseWrapper<?> writeValue(HerculesWritable in) {
        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.INSERT);
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setEventType(CanalEntry.EventType.INSERT);
        headerBuilder.setSourceType(CanalEntry.Type.MYSQL);
        headerBuilder.setExecuteTime(System.currentTimeMillis());

        headerBuilder.setSchemaName(options.getString(CanalMysqlOutputOptionConf.SCHEMA_NAME, ""));
        headerBuilder.setTableName(options.getString(CanalMysqlOutputOptionConf.TABLE_NAME, ""));

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        for (Map.Entry<String, BaseWrapper<?>> column : in.entrySet()) {
            String columnName = column.getKey();
            BaseWrapper<?> columnValue = column.getValue();

            DataType type = schema.getColumnTypeMap().getOrDefault(columnName, columnValue.getType());
            CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder()
                    .setName(columnName)
                    .setSqlType(dataTypeConverter.getElementType(type));
            try {
                getWrapperSetter(type).set(columnValue, columnBuilder, null, null, 0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            columnBuilder.setIsKey(isKey(columnName, schema.getUniqueKeyGroupList()));
            rowDataBuilder.addAfterColumns(columnBuilder.build());
        }
        rowChangeBuilder.addRowDatas(rowDataBuilder.build());
        return BytesWrapper.get(
                serializeCanalEntry(
                        CanalEntry.Entry.newBuilder()
                                .setHeader(headerBuilder.build())
                                .setEntryType(CanalEntry.EntryType.ROWDATA)
                                .setStoreValue(rowChangeBuilder.build().toByteString())
                                .build()
                )
        );
    }
}
