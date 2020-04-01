package com.xiaohongshu.db.hercules.rdbms.input.mr.splitter;

import java.math.BigDecimal;

/**
 * TODO 大小写问题怎么解决
 */
public class HexTextSplitter extends TextSplitter {
    public HexTextSplitter(boolean nVarchar) {
        super(nVarchar);
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
