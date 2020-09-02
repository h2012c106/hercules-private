package com.xiaohongshu.db.hercules.rdbms.mr.input.splitter;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateSplitter extends BaseSplitter<Date> {

    public DateSplitter(ResultSet minMaxCountResult) throws SQLException {
        super(minMaxCountResult);
    }

    @Override
    public ResultSetGetter<Date> getResultSetGetter() {
        return new ResultSetGetter<Date>() {

            private SQLException throwException(SQLException e) throws SQLException {
                String regex = "^Value '(.+)' can not be represented as java.sql.Timestamp$";
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(e.getMessage());
                if (matcher.find()) {
                    throw new MapReduceException(
                            String.format("The unparsable & uncomparable timestamp value [%s] will cause split error, " +
                                            "please change the split-by col or if you insist using this column, " +
                                            "set '--%s 1' and try again. " +
                                            "(Don't worry, the illegal time value will only be restricted when used to split, " +
                                            "the value will be safe and sound when transferred as a part of row)",
                                    matcher.group(1), CommonOptionsConf.NUM_MAPPER), e
                    );
                } else {
                    return e;
                }
            }

            @Override
            public Date get(ResultSet resultSet, int seq) throws SQLException {
                try {
                    return resultSet.getTimestamp(seq);
                } catch (SQLException e) {
                    throw throwException(e);
                }
            }

            @Override
            public Date get(ResultSet resultSet, String name) throws SQLException {
                try {
                    return resultSet.getTimestamp(name);
                } catch (SQLException e) {
                    throw throwException(e);
                }
            }
        };
    }

    @Override
    protected BigDecimal convertToDecimal(Date value) {
        return BigDecimal.valueOf(value.getTime());
    }

    @Override
    protected Date convertFromDecimal(BigDecimal value) {
        return new Date(value.longValue());
    }

    @Override
    protected Enclosing quote() {
        return new Enclosing("'", "'");
    }
}
