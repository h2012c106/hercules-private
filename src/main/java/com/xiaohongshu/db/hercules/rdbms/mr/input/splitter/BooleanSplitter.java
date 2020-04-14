package com.xiaohongshu.db.hercules.rdbms.mr.input.splitter;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BooleanSplitter extends BaseSplitter<Boolean> {

    public BooleanSplitter(ResultSet minMaxCountResult) throws SQLException {
        super(minMaxCountResult);
    }

    @Override
    public ResultSetGetter<Boolean> getResultSetGetter() {
        return new ResultSetGetter<Boolean>() {
            @Override
            public Boolean get(ResultSet resultSet, int seq) throws SQLException {
                return resultSet.getBoolean(seq);
            }
        };
    }

    @Override
    protected BigDecimal convertToDecimal(Boolean value) {
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    @Override
    protected Boolean convertFromDecimal(BigDecimal value) {
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return false;
        } else if (value.compareTo(BigDecimal.ZERO) > 0 && value.compareTo(BigDecimal.ONE) <= 0) {
            return true;
        } else {
            throw new MapReduceException("Unsupposed existing the decimal value converted from boolean: "
                    + value.toPlainString());
        }
    }
}
