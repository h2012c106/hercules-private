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

        @Override
        public Long get(ResultSet resultSet, String name) throws SQLException {
            return resultSet.getLong(name);
        }
    };
    public static final ResultSetGetter<String> STRING_GETTER = new ResultSetGetter<String>() {
        @Override
        public String get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getString(seq);
        }

        @Override
        public String get(ResultSet resultSet, String name) throws SQLException {
            return resultSet.getString(name);
        }
    };

    abstract public T get(ResultSet resultSet, int seq) throws SQLException;

    abstract public T get(ResultSet resultSet, String name) throws SQLException;
}
