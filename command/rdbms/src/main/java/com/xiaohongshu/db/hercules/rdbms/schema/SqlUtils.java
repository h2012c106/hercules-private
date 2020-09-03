package com.xiaohongshu.db.hercules.rdbms.schema;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class SqlUtils {

    private static final Log LOG = LogFactory.getLog(SqlUtils.class);

    private static final String COLUMN_NAME_ENCLOSING = "`";
    private static final Pattern COLUMN_ENCLOSED_PATTERN = Pattern.compile(COLUMN_NAME_ENCLOSING + "\\S+" + COLUMN_NAME_ENCLOSING);

    public static String encloseColumnName(@NonNull String columnName) {
        if ("*".equals(columnName) || COLUMN_ENCLOSED_PATTERN.matcher(columnName).matches()) {
            return columnName;
        } else {
            return COLUMN_NAME_ENCLOSING + columnName + COLUMN_NAME_ENCLOSING;
        }
    }

    private static SQLSelectStatement parse(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        if (statement instanceof SQLSelectStatement) {
            return (SQLSelectStatement) statement;
        } else {
            throw new RuntimeException("Wrong type sql, expect SELECT: " + sql);
        }
    }

    /**
     * 制作列元素，e.g. update_time
     *
     * @param columnName
     * @return
     */
    public static SQLSelectItem makeItem(String columnName) {
        columnName = encloseColumnName(columnName);
        return new SQLSelectItem(new SQLIdentifierExpr(columnName));
    }

    /**
     * 制作对列的函数元素，e.g. sum(a)
     *
     * @param methodName
     * @param columnName
     * @return
     */
    public static SQLSelectItem makeItem(String methodName, String columnName) {
        columnName = encloseColumnName(columnName);
        return new SQLSelectItem(new SQLAggregateExpr(methodName,
                null,
                new SQLIdentifierExpr(columnName)));
    }

    /**
     * 制作对数字的函数元素，e.g. count(1)
     *
     * @param methodName
     * @param number
     * @return
     */
    public static SQLSelectItem makeItem(String methodName, Integer number) {
        return new SQLSelectItem(new SQLAggregateExpr(methodName,
                null,
                new SQLIntegerExpr(number)));
    }

    private static SQLSelectItem[] convert(String... strs) {
        return Arrays.stream(strs).map(SqlUtils::makeItem).toArray(SQLSelectItem[]::new);
    }

    public static String addSelectItem(String sql, String... selectItemStrs) {
        return addSelectItem(sql, convert(selectItemStrs));
    }

    public static String addSelectItem(String sql, SQLSelectItem... selectItems) {
        SQLSelectStatement statement = parse(sql);
        for (SQLSelectItem item : selectItems) {
            statement.getSelect().getQueryBlock().addSelectItem(item);
        }
        return statement.toString();
    }

    public static String replaceSelectItem(String sql, String... selectItemStrs) {
        return replaceSelectItem(sql, convert(selectItemStrs));
    }

    public static String replaceSelectItem(String sql, SQLSelectItem... selectItems) {
        SQLSelectStatement statement = parse(sql);
        List<SQLSelectItem> selectList = statement.getSelect().getQueryBlock().getSelectList();
        selectList.clear();
        for (SQLSelectItem item : selectItems) {
            statement.getSelect().getQueryBlock().addSelectItem(item);
        }
        return statement.toString();
    }

    public static String addWhere(String sql, String... wheres) {
        SQLSelectStatement statement = parse(sql);
        for (String where : wheres) {
            statement.getSelect().getQueryBlock().addCondition(where);
        }
        return statement.toString();
    }

    public static ExtendedDate getTimestamp(ResultSet resultSet, int seq) throws SQLException {
        try {
            Timestamp res = resultSet.getTimestamp(seq);
            return res == null ? null : ExtendedDate.initialize(res);
        } catch (SQLException e) {
            if (StringUtils.contains(e.getMessage(), "0000")) {
                return ExtendedDate.ZERO_INSTANCE;
            } else {
                throw e;
            }
        }
    }

    public static String addNullCondition(String query, String column, boolean isNull) {
        return SqlUtils.addWhere(query, String.format("%s IS %s NULL", column, isNull ? "" : "NOT"));
    }

    /**
     * 由于迭代器特性，且会关闭资源，故是一次性的，再想执行需要重新生成resultSet
     *
     * @param in
     * @param seq
     * @param resultSetGetter
     * @param <T>
     * @return
     * @throws SQLException
     */
    public static <T> List<T> resultSetToList(@NonNull ResultSet in, int seq, ResultSetGetter<T> resultSetGetter, boolean close)
            throws SQLException {
        List<T> out = new ArrayList<>();
        try {
            while (in.next()) {
                // 不可能出现null
                out.add(resultSetGetter.get(in, seq));
            }
            return out;
        } finally {
            if (close) {
                try {
                    in.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing resultset: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    /**
     * 由于迭代器特性，且会关闭资源，故是一次性的，再想执行需要重新生成resultSet
     *
     * @param in
     * @param name
     * @param resultSetGetter
     * @param <T>
     * @return
     * @throws SQLException
     */
    public static <T> List<T> resultSetToList(@NonNull ResultSet in, String name, ResultSetGetter<T> resultSetGetter, boolean close)
            throws SQLException {
        List<T> out = new ArrayList<>();
        try {
            while (in.next()) {
                // 不可能出现null
                out.add(resultSetGetter.get(in, name));
            }
            return out;
        } finally {
            if (close) {
                try {
                    in.close();
                } catch (SQLException e) {
                    LOG.warn("SQLException closing resultset: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    public static void release(List<AutoCloseable> closeableList) {
        for (AutoCloseable item : closeableList) {
            if (item != null) {
                try {
                    item.close();
                } catch (Exception e) {
                    LOG.warn("Exception when releasing: " + ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    public static PreparedStatement makeReadStatement(Connection connection, String sql) throws SQLException {
        return makeReadStatement(connection, sql, null);
    }

    public static PreparedStatement makeReadStatement(Connection connection, String sql, Integer fetchSize) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize != null) {
            LOG.info("Using fetchSize for query: " + fetchSize);
        } else {
            LOG.warn("The fetch size is set to null. To avoid OOM, use client side cursor, set fetch size to Integer.MIN_VALUE.");
            fetchSize = Integer.MIN_VALUE;
        }
        statement.setFetchSize(fetchSize);
        return statement;
    }
}
