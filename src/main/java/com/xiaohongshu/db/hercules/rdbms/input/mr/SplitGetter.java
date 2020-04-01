package com.xiaohongshu.db.hercules.rdbms.input.mr;

import com.xiaohongshu.db.hercules.rdbms.input.mr.splitter.BaseSplitter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.hadoop.mapreduce.InputSplit;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface SplitGetter {

    List<InputSplit> getSplits(ResultSet minMaxCountResult, int numSplits, String splitBy,
                               RDBMSSchemaFetcher schemaFetcher, BaseSplitter splitter,
                               BigDecimal maxSampleRow) throws SQLException;

}
