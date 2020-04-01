package com.xiaohongshu.db.hercules.rdbms.schema;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlUtils {

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

    public static String addSelectItem(String sql, SQLSelectItem... selectItems) {
        SQLSelectStatement statement = parse(sql);
        for (SQLSelectItem item : selectItems) {
            statement.getSelect().getQueryBlock().addSelectItem(item);
        }
        return statement.toString();
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

    public static String getTimestamp(ResultSet resultSet, int seq) throws SQLException {
        try {
            return resultSet.getString(seq);
        } catch (SQLException e) {
            String regex = "^Value '(.+)' can not be represented as java.sql.Timestamp$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(e.getMessage());
            if (matcher.find()) {
                return matcher.group(1);
            } else {
                throw e;
            }
        }
    }

    /**
     * @param resultSet
     * @param seq
     * @param defaultValue 仅当是0000-00-00 00:00:00时会返回的默认值
     * @return
     * @throws SQLException
     */
    public static Long getTimestamp(ResultSet resultSet, int seq, Long defaultValue) throws SQLException {
        try {
            Timestamp ts = resultSet.getTimestamp(seq);
            return ts == null ? null : ts.getTime();
        } catch (SQLException e) {
            String regex = "^Value '(.+)' can not be represented as java.sql.Timestamp$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(e.getMessage());
            if (matcher.find()) {
                return defaultValue;
            } else {
                throw e;
            }
        }
    }
}
