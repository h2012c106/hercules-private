package com.xiaohongshu.db.hercules.core.serialize;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.MapWrapper;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 内部由Map存储，摈弃Array。在10亿行数据下，Map的写比List慢300秒，读比List慢100秒。
 * 10亿行的整体传输时间至少要半天，400秒的差额一定程度上是可以忽略不计的，至少如果出现瓶颈，不太可能是这个改动导致的。
 */
public class HerculesWritable implements Writable {

    private MapWrapper row;

    private long byteSize;

    private static JSONObject columnNameMap;

    public static void setColumnNameMap(JSONObject columnNameMap) {
        HerculesWritable.columnNameMap = columnNameMap;
    }

    private static boolean targetOneLevel;

    public static void setTargetOneLevel(boolean targetOneLevel) {
        HerculesWritable.targetOneLevel = targetOneLevel;
    }

    public HerculesWritable() {
        this(1);
    }

    public HerculesWritable(int columnNum) {
        row = new MapWrapper(columnNum);
        byteSize = 0;
    }

    private String mapColumnName(String columnName) {
        return (String) columnNameMap.getOrDefault(columnName, columnName);
    }

    public void put(String columnName, BaseWrapper column) {
        // 把上游列名映成下游列名
        columnName = mapColumnName(columnName);
        // 已经是下游列名了，自然按照下游是否存在嵌套列来判断
        row.put(columnName, column, targetOneLevel);
        byteSize += column.getByteSize();
    }

    public BaseWrapper get(String columnName) {
        return row.get(columnName, targetOneLevel);
    }

    /**
     * 把一行的数据根据某个List列展成一/多行
     *
     * @param splitBaseColumn
     * @return
     */
    public List<HerculesWritable> split(List<String> splitBaseColumn) {
        splitBaseColumn = splitBaseColumn.stream().map(this::mapColumnName).collect(Collectors.toList());
        return null;
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
