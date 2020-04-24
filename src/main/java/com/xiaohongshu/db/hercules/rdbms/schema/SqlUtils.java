package com.xiaohongshu.db.hercules.rdbms.schema;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private static SQLSelectItem[] convert(String... strs){
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

    public static String getTimestamp(ResultSet resultSet, int seq) throws SQLException {
        try {
            return resultSet.getString(seq);
        } catch (SQLException e) {
            List<String> regexList = Lists.newArrayList(
                    "^Value '(.+)' can not be represented as java.sql.Timestamp$"
            );
            for (String regex : regexList) {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(e.getMessage());
                if (matcher.find()) {
                    return "0000-00-00 00:00:00";
                }
            }
            throw e;
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
            String strRes = resultSet.getString(seq);
            if (strRes.startsWith("0000-00-00 00:00:00")) {
                return defaultValue;
            }
            Timestamp ts = resultSet.getTimestamp(seq);
            return ts == null ? null : ts.getTime();
        } catch (SQLException e) {
            List<String> regexList = Lists.newArrayList(
                    "^Value '(.+)' can not be represented as java.sql.Timestamp$"
            );
            for (String regex : regexList) {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(e.getMessage());
                if (matcher.find()) {
                    return defaultValue;
                }
            }
            throw e;
        }
    }

    public static String addNullCondition(String query, String column, boolean isNull) {
        return SqlUtils.addWhere(query, String.format("%s IS %s NULL", column, isNull ? "" : "NOT"));
    }

    private static String makeQueryByParts(String tableName, List<String> columnNameList, String condition) {
        return String.format("SELECT %s FROM %s %s",
                String.join(", ", columnNameList),
                tableName,
                condition);
    }


    public static String makeBaseQuery(GenericOptions options) {
        return makeBaseQuery(options, false);
    }

    /**
     * @param options
     * @param useAsterisk 使用星号去查询，当且仅当schema fetcher获取全部列时为true，在其他情况下column必已被定义好
     * @return
     */
    public static String makeBaseQuery(GenericOptions options, boolean useAsterisk) {
        if (options.hasProperty(RDBMSInputOptionsConf.QUERY)) {
            return options.getString(RDBMSInputOptionsConf.QUERY, null);
        } else {
            List<String> columnNameList;
            if (!options.hasProperty(BaseDataSourceOptionsConf.COLUMN)) {
                if (useAsterisk) {
                    columnNameList = Lists.newArrayList("*");
                } else {
                    throw new RuntimeException();
                }
            } else {
                columnNameList
                        = Arrays.asList(options.getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
            }
            String where = options.hasProperty(RDBMSInputOptionsConf.CONDITION)
                    ? " WHERE " + options.getString(RDBMSInputOptionsConf.CONDITION, null)
                    : "";
            String table = options.getString(RDBMSOptionsConf.TABLE, null);
            return makeQueryByParts(table, columnNameList, where);
        }
    }



}
