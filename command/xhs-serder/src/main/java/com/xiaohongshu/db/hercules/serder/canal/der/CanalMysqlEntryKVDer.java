package com.xiaohongshu.db.hercules.serder.canal.der;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serder.KVDer;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.rdbms.schema.ColumnInfo;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public abstract class CanalMysqlEntryKVDer extends KVDer<CanalEntry.Column> implements InjectedClass {

    @SchemaInfo
    private Schema schema;

    @Assembly
    private RDBMSDataTypeConverter dataTypeConverter;

    private Set<String> columnNameSet;

    public CanalMysqlEntryKVDer() {
        super(new CanalMysqlWrapperGetterFactory());
    }

    @Override
    public void afterInject() {
        super.afterInject();
        columnNameSet = new HashSet<>(schema.getColumnNameList());
    }

    abstract protected List<CanalEntry.Entry> deserializeCanalEntry(byte[] bytes);

    @Override
    protected List<MapWrapper> readValue(BaseWrapper<?> inValue) throws IOException, InterruptedException {
        List<CanalEntry.Entry> entryList = deserializeCanalEntry(inValue.asBytes());
        List<MapWrapper> res = new LinkedList<>();
        for (CanalEntry.Entry entry : entryList) {
            // 事务信息不作同步
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                return null;
            }
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR. Parse of event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // TODO 硬要同步应该也可以，但又更新又插策略对于一个"批同步"工具而言可能就有点"流"有点复杂了，先放着，还没想明白
                if (eventType != CanalEntry.EventType.INSERT) {
                    return null;
                }

                MapWrapper record = new MapWrapper(rowChange.getRowDatas(0).getAfterColumnsList().size());

                List<CanalEntry.Column> columns = rowData.getAfterColumnsList();

                for (CanalEntry.Column column : columns) {
                    String columnName = column.getName();
                    // 筛选往下游传的列
                    if (!CollectionUtils.isEmpty(columnNameSet) && columnNameSet.contains(columnName)) {
                        DataType type = dataTypeConverter.convertElementType(new ColumnInfo(column.getSqlType()));
                        try {
                            record.put(column.getName(), getWrapperGetter(type).get(column, null, column.getName(), 0));
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                    }
                }

                res.add(record);
            }
        }
        return res;
    }
}
