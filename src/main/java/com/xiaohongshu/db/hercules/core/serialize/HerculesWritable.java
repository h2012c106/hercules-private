package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HerculesWritable implements Writable {

    /**
     * 列值列表，按照上游列顺序。
     * 用列值列表+[目标列名-列表下标]map来做比[目标列名-列值]map+列映射map读写总效率大约高5%，前者set快，后者get快
     */
    private BaseWrapper[] columnValueList;
    private int tmpSize = 0;

    /**
     * 下游列名到{@link #columnValueList}下标的映射
     */
    private static Map<String, Integer> targetColumnNameToColumnValueListSeqMap;

    private int byteSize;

    public HerculesWritable() {
        this(1);
    }

    public HerculesWritable(int columnNum) {
        columnValueList = new BaseWrapper[columnNum];
        byteSize = 0;
    }

    /**
     * @param columnNameMap        上下游列名映射，key上游名，value下游名
     * @param sourceColumnNameList 上游列顺序
     */
    public static void setTargetColumnNameToColumnValueListSeqMap(Map<String, String> columnNameMap,
                                                                  List<String> sourceColumnNameList) {
        if (targetColumnNameToColumnValueListSeqMap == null) {
            synchronized (HerculesWritable.class) {
                if (targetColumnNameToColumnValueListSeqMap == null) {
                    int initialSize = (int) ((float) sourceColumnNameList.size() / 0.75F + 1.0F);
                    targetColumnNameToColumnValueListSeqMap = new HashMap<>(initialSize);
                    for (int i = 0; i < sourceColumnNameList.size(); ++i) {
                        String sourceColumnName = sourceColumnNameList.get(i);
                        String targetColumnName = columnNameMap.getOrDefault(sourceColumnName, sourceColumnName);
                        // 可以确定targetColumnName是没有重复的
                        targetColumnNameToColumnValueListSeqMap.put(targetColumnName, i);
                    }
                }
            }
        }
    }

    public void append(BaseWrapper column) {
        columnValueList[tmpSize++] = column;
        byteSize += column.getByteSize();
    }

    public BaseWrapper get(String columnName) {
        if (targetColumnNameToColumnValueListSeqMap.containsKey(columnName)) {
            return columnValueList[targetColumnNameToColumnValueListSeqMap.get(columnName)];
        } else {
            return null;
        }
    }

    public BaseWrapper get(Integer seq) {
        if (seq == null) {
            return null;
        } else {
            return columnValueList[seq];
        }
    }

    public int getByteSize() {
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
                .append("columnValueList", columnValueList)
                .toString();
    }
}
