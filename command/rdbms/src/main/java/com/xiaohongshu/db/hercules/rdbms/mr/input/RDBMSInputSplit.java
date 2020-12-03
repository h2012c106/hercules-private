package com.xiaohongshu.db.hercules.rdbms.mr.input;

import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;

public class RDBMSInputSplit extends DataDrivenDBInputFormat.DataDrivenDBInputSplit {

    public RDBMSInputSplit() {
        super("1 = 1", "1 = 1");
    }

    public RDBMSInputSplit(String lower, String upper) {
        super(lower == null ? "1 = 1" : lower, upper == null ? "1 = 1" : upper);
    }

    @Override
    public String toString() {
        return getLowerClause() + " AND " + getUpperClause();
    }
}
