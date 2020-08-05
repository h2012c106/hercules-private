package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import lombok.NonNull;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 内部由Map存储，摈弃Array。在10亿行数据下，Map的写比List慢300秒，读比List慢100秒。
 * 10亿行的整体传输时间至少要半天，400秒的差额一定程度上是可以忽略不计的，至少如果出现瓶颈，不太可能是这个改动导致的。
 */
public class HerculesWritable implements Writable {

    private final MapWrapper row;

    private long byteSize;

    public HerculesWritable() {
        this(1);
    }

    public HerculesWritable(int columnNum) {
        row = new MapWrapper(columnNum);
        byteSize = 0;
    }

    public HerculesWritable(MapWrapper row) {
        this.row = row;
        byteSize = row.getByteSize();
    }

    public void put(String columnName, BaseWrapper<?> column) {
        row.put(columnName, column);
        byteSize += column.getByteSize();
    }

    public BaseWrapper<?> get(@NonNull String columnName) {
        return row.get(columnName);
    }

    public Set<Map.Entry<String, BaseWrapper<?>>> entrySet() {
        return row.entrySet();
    }

    public MapWrapper getRow() {
        return row;
    }

    public long getByteSize() {
        return byteSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("row", row)
                .toString();
    }
}
