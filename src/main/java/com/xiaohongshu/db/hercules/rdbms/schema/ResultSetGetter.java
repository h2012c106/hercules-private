package com.xiaohongshu.db.hercules.rdbms.schema;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class ResultSetGetter<T> {
    public static final ResultSetGetter<Long> LONG_GETTER = new ResultSetGetter<Long>() {
        @Override
        public Long get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getLong(seq);
        }
    };
    public static final ResultSetGetter<BigDecimal> DOUBLE_GETTER = new ResultSetGetter<BigDecimal>() {
        @Override
        public BigDecimal get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getBigDecimal(seq);
        }
    };
    public static final ResultSetGetter<String> STRING_GETTER = new ResultSetGetter<String>() {
        @Override
        public String get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getString(seq);
        }
    };
    public static final ResultSetGetter<Boolean> BOOLEAN_GETTER = new ResultSetGetter<Boolean>() {
        @Override
        public Boolean get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getBoolean(seq);
        }
    };
    public static final ResultSetGetter<String> DATE_GETTER = new ResultSetGetter<String>() {
        @Override
        public String get(ResultSet resultSet, int seq) throws SQLException {
            return SqlUtils.getTimestamp(resultSet, seq);
        }
    };
    public static final ResultSetGetter<byte[]> BYTES_GETTER = new ResultSetGetter<byte[]>() {
        @Override
        public byte[] get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getBytes(seq);
        }
    };

    abstract public T get(ResultSet resultSet, int seq) throws SQLException;
}
