package com.xiaohongshu.db.hercules.rdbms.input.mr;

import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;

public class RDBMSInputSplit extends DataDrivenDBInputFormat.DataDrivenDBInputSplit {

    public RDBMSInputSplit() {
    }

    public RDBMSInputSplit(String lower, String upper) {
        super(lower, upper);
    }

    @Override
    public String toString() {
        return getLowerClause() + " AND " + getUpperClause();
    }
}
