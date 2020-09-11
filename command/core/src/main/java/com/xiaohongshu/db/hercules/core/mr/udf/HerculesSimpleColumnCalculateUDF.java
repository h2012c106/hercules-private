package com.xiaohongshu.db.hercules.core.mr.udf;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class HerculesSimpleColumnCalculateUDF extends HerculesUDF {

    private final List<ColumnCalculateInfo> columnCalculateInfoList;
    private final List<String> deleteColumnList;

    protected HerculesSimpleColumnCalculateUDF() {
        this.columnCalculateInfoList = createColumnCalculateInfoList();
        this.deleteColumnList = this.columnCalculateInfoList
                .stream()
                .filter(ColumnCalculateInfo::isDeleteSourceColumn)
                .map(info -> Arrays.asList(info.getInColumnNames()))
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public void initialize(Mapper.Context context) throws IOException, InterruptedException {
    }

    abstract protected List<ColumnCalculateInfo> createColumnCalculateInfoList();

    @Override
    public final HerculesWritable evaluate(HerculesWritable row) throws IOException, InterruptedException {
        final MapWrapper rowWrapper = row.getRow();
        for (ColumnCalculateInfo calculateInfo : columnCalculateInfoList) {
            String[] inColumns = calculateInfo.getInColumnNames();
            inColumns = inColumns == null ? new String[0] : inColumns;
            BaseWrapper<?> newColumn = calculateInfo.getCalculator().apply(
                    Arrays.stream(inColumns)
                            .map(columnName -> WritableUtils.get(rowWrapper, columnName))
                            .toArray(BaseWrapper<?>[]::new)
            );
            WritableUtils.put(rowWrapper, calculateInfo.getOutColumnName(), newColumn);
        }
        for (String columnName : deleteColumnList) {
            WritableUtils.remove(rowWrapper, columnName);
        }
        return row;
    }

    @Override
    public void close() throws IOException, InterruptedException {
    }

    protected static class ColumnCalculateInfo {
        private final String[] inColumnNames;
        private final String outColumnName;
        /**
         * 入参个数==inColumnNames.length
         */
        private final Function<BaseWrapper<?>[], BaseWrapper<?>> calculator;
        private final boolean deleteSourceColumn;

        public ColumnCalculateInfo(String[] inColumnNames, String outColumnName, Function<BaseWrapper<?>[], BaseWrapper<?>> calculator) {
            this(inColumnNames, outColumnName, calculator, false);
        }

        public ColumnCalculateInfo(String[] inColumnNames, String outColumnName, Function<BaseWrapper<?>[], BaseWrapper<?>> calculator, boolean deleteSourceColumn) {
            this.inColumnNames = inColumnNames;
            this.outColumnName = outColumnName;
            this.calculator = calculator;
            this.deleteSourceColumn = deleteSourceColumn;
        }

        public String[] getInColumnNames() {
            return inColumnNames;
        }

        public String getOutColumnName() {
            return outColumnName;
        }

        public Function<BaseWrapper<?>[], BaseWrapper<?>> getCalculator() {
            return calculator;
        }

        public boolean isDeleteSourceColumn() {
            return deleteSourceColumn;
        }
    }

}
