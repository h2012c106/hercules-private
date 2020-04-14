package com.xiaohongshu.db.hercules.rdbms.mr.input.splitter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TODO 大小写问题怎么解决
 */
public class HexTextSplitter extends TextSplitter {
    public HexTextSplitter(ResultSet minMaxCountResult, boolean nVarchar) throws SQLException {
        super(minMaxCountResult, nVarchar);
        throw new UnsupportedOperationException("Not support hex text split yet, " +
                "need figure out how to deal with case sensitive.");
    }

    @Override
    protected void setCommonPrefix(String minVal, String maxVal) {
    }

    @Override
    protected BigDecimal convertToDecimal(String value) {
        return BigDecimal.valueOf(Long.parseLong(value, 16));
    }

    @Override
    protected String convertFromDecimal(BigDecimal value) {
        return Long.toHexString(value.longValue());
    }
}
