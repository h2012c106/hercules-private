package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.rdbms.mr.input.splitter.BaseSplitter;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.hadoop.mapreduce.InputSplit;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class RDBMSFastSplitterGetter implements SplitGetter {

    @Override
    public List<InputSplit> getSplits(ResultSet minMaxCountResult, int numSplits, String splitBy,
                                      Map<String, DataType> columnTypeMap, String baseSql, BaseSplitter splitter,
                                      BigDecimal maxSampleRow, RDBMSManager manager) throws SQLException {
        return splitter.getFastSplitPoint(splitBy, numSplits);
    }
}
