package com.xiaohongshu.db.hercules.rdbms.mr.input.splitter;

import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DoubleSplitter extends BaseSplitter<BigDecimal> {
    public DoubleSplitter(ResultSet minMaxCountResult) throws SQLException {
        super(minMaxCountResult);
    }

    @Override
    public ResultSetGetter<BigDecimal> getResultSetGetter() {
        return new ResultSetGetter<BigDecimal>() {
            @Override
            public BigDecimal get(ResultSet resultSet, int seq) throws SQLException {
                return resultSet.getBigDecimal(seq);
            }

            @Override
            public BigDecimal get(ResultSet resultSet, String name) throws SQLException {
                return resultSet.getBigDecimal(name);
            }
        };
    }

    @Override
    protected BigDecimal convertToDecimal(BigDecimal value) {
        return value;
    }

    @Override
    protected BigDecimal convertFromDecimal(BigDecimal value) {
        return value;
    }
}
