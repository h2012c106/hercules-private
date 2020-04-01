package com.xiaohongshu.db.hercules.rdbms.input.mr.splitter;

import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IntegerSplitter extends BaseSplitter<Long> {

    @Override
    public ResultSetGetter<Long> getResultSetGetter() {
        return new ResultSetGetter<Long>() {
            @Override
            public Long get(ResultSet resultSet, int seq) throws SQLException {
                return resultSet.getLong(seq);
            }
        };
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
