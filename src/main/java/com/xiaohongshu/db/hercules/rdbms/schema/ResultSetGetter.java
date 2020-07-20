package com.xiaohongshu.db.hercules.rdbms.schema;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class ResultSetGetter<T> {
    public static final ResultSetGetter<Integer> INT_GETTER = new ResultSetGetter<Integer>() {
        @Override
        public Integer get(ResultSet resultSet, int seq) throws SQLException {
            return resultSet.getInt(seq);
        }

        @Override
        public Integer get(ResultSet resultSet, String name) throws SQLException {
            return resultSet.getInt(name);
        }
    };
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
