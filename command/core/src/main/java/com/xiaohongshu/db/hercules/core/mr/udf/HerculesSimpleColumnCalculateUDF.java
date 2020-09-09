package com.xiaohongshu.db.hercules.core.mr.udf;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public abstract class HerculesSimpleColumnCalculateUDF extends HerculesUDF {

    private final List<ColumnCalculateInfo> columnCalculateInfoList;

    protected HerculesSimpleColumnCalculateUDF() {
        this.columnCalculateInfoList = createColumnCalculateInfoList();
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

        public ColumnCalculateInfo(String[] inColumnNames, String outColumnName, Function<BaseWrapper<?>[], BaseWrapper<?>> calculator) {
            this.inColumnNames = inColumnNames;
            this.outColumnName = outColumnName;
            this.calculator = calculator;
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
    }

}
