package com.xiaohongshu.db.hercules.rdbms.mr.input.splitter;

import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IntegerSplitter extends BaseSplitter<Long> {

    public IntegerSplitter(ResultSet minMaxCountResult) throws SQLException {
        super(minMaxCountResult);
    }

    @Override
    public ResultSetGetter<Long> getResultSetGetter() {
        return ResultSetGetter.LONG_GETTER;
    }

    @Override
    protected BigDecimal convertToDecimal(Long value) {
        return BigDecimal.valueOf(value);
    }

    @Override
    protected Long convertFromDecimal(BigDecimal value) {
        return value.longValue();
    }
}
