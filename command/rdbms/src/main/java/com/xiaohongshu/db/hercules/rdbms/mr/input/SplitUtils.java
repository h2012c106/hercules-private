package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.rdbms.mr.input.splitter.*;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class SplitUtils {

    private static final Log LOG = LogFactory.getLog(SplitUtils.class);

    private static BaseSplitter getSplitter(ResultSet minMaxCountResult,
                                            DataType dataType, int sqlDataType, boolean hexString) throws SQLException {
        if (!dataType.isCustom()) {
            switch ((BaseDataType) dataType) {
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                case LONGLONG:
                    return new IntegerSplitter(minMaxCountResult);
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                    return new DoubleSplitter(minMaxCountResult);
                case BOOLEAN:
                    return new BooleanSplitter(minMaxCountResult);
                case DATE:
                case TIME:
                case DATETIME:
                    return new DateSplitter(minMaxCountResult);
                case STRING:
                    boolean nvarchar;
                    switch (sqlDataType) {
                        case Types.NVARCHAR:
                        case Types.NCHAR:
                            nvarchar = true;
                            break;
                        default:
                            nvarchar = false;
                    }
                    if (hexString) {
                        return new HexTextSplitter(minMaxCountResult, nvarchar);
                    } else {
                        return new TextSplitter(minMaxCountResult, nvarchar);
                    }
                default:
                    throw new UnsupportedOperationException("Unsupported data type to split: " + dataType.toString());
            }
        } else {
            throw new UnsupportedOperationException("Unsupported data type to split: " + dataType.toString());
        }
    }

    public static SplitResult split(GenericOptions sourceOptions, Schema schema, RDBMSSchemaFetcher schemaFetcher,
                                    int numSplits, RDBMSManager manager, String baseSql, SplitGetter splitGetter) throws IOException {
        // 检查split-by列在不在列集里
        String splitBy;
        splitBy = sourceOptions.getString(RDBMSInputOptionsConf.SPLIT_BY, null);
        if (splitBy == null) {
            List<Set<String>> length1Index = schema.getIndexGroupList()
                    .stream()
                    .filter(set -> set.size() == 1)
                    .collect(Collectors.toList());
            if (length1Index.size() > 0) {
                splitBy = length1Index.get(0).iterator().next();
                LOG.info("Use a index column as split-by key: " + splitBy);
            }
        }
        if (splitBy == null) {
            throw new SchemaException(String.format("Cannot get the split-by column automatically, " +
                    "please use '--%s' to specify.", RDBMSInputOptionsConf.SPLIT_BY));
        }

        // 如果只有一个split，那下面的不用做了
        if (numSplits == 1) {
            LOG.warn("Map set to 1, only use 1 map.");
            return new SplitResult(BaseSplitter.generateAllSplit(), 0);
        }

        // 检查key上是否有索引
        boolean ignoreCheckKey = sourceOptions.getBoolean(RDBMSInputOptionsConf.IGNORE_SPLIT_KEY_CHECK, false);
        if (!ignoreCheckKey && !schema.getIndexGroupList().contains(Sets.newHashSet(splitBy))) {
            throw new RuntimeException(String.format("Cannot specify a non-key split key [%s]. If you insist, please use '--%s'.", splitBy, RDBMSInputOptionsConf.IGNORE_SPLIT_KEY_CHECK));
        }

        ResultSet minMaxCountResult = null;
        Statement minMaxCountStatement = null;
        Connection connection = null;
        try {
            connection = manager.getConnection();

            minMaxCountStatement = connection.createStatement();

            String minMaxCountSql = SqlUtils.replaceSelectItem(baseSql,
                    SqlUtils.makeItem("MIN", splitBy),
                    SqlUtils.makeItem("MAX", splitBy),
                    SqlUtils.makeItem("COUNT", 1));
            minMaxCountSql = SqlUtils.addNullCondition(minMaxCountSql, splitBy, false);
            LOG.info("The min+max+count not null sql is: " + minMaxCountSql);

            minMaxCountResult = minMaxCountStatement.executeQuery(minMaxCountSql);
            minMaxCountResult.next();

            // Based on the type of the firstResults, use a different mechanism
            // for interpolating split points (i.e., numeric splits, text splits,
            // dates, etc.)
            int sqlDataType = minMaxCountResult.getMetaData().getColumnType(1);
            boolean isSigned = minMaxCountResult.getMetaData().isSigned(1);

            // MySQL has an unsigned integer which we need to allocate space for
            if (sqlDataType == Types.INTEGER && !isSigned) {
                sqlDataType = Types.BIGINT;
            }

            BaseSplitter splitter = getSplitter(
                    minMaxCountResult,
                    schema.getColumnTypeMap().get(splitBy),
                    sqlDataType,
                    sourceOptions.getBoolean(RDBMSInputOptionsConf.SPLIT_BY_HEX_STRING, false)
            );

            // 如果没有不null的行，那么直接返回一个split
            long notNullRowNum = minMaxCountResult.getLong(3);
            LOG.info("Not null row num: " + notNullRowNum);
            if (notNullRowNum == 0) {
                LOG.warn("Full of null! Use 1 map.");
                return new SplitResult(BaseSplitter.generateNullSplit(splitBy), 0);
            }

            String nullSql = SqlUtils.replaceSelectItem(baseSql,
                    SqlUtils.makeItem("COUNT", 1));
            nullSql = SqlUtils.addNullCondition(nullSql, splitBy, true);
            LOG.info("The count null sql is: " + nullSql);

            long nullRowNum = manager.executeSelect(nullSql, 1, ResultSetGetter.LONG_GETTER).get(0);
            LOG.info("Null row num: " + nullRowNum);

            BigDecimal maxSampleRow = sourceOptions
                    .getDecimal(RDBMSInputOptionsConf.BALANCE_SPLIT_SAMPLE_MAX_ROW, null);

            List<InputSplit> res = splitGetter.getSplits(minMaxCountResult, numSplits, splitBy, schema.getColumnTypeMap(),
                    baseSql, splitter, maxSampleRow, manager);

            if (nullRowNum > 0) {
                res.addAll(BaseSplitter.generateNullSplit(splitBy));
            }

            // timestamp型的split by列如果带0000-00-00 00:00:00出去的split会变成0002-11-30 00:00:00
            int splitBySqlType = schemaFetcher.getColumnSqlType(baseSql, splitBy);
            if (splitBySqlType == Types.TIMESTAMP || splitBySqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
                String zeroDateSql = SqlUtils.replaceSelectItem(baseSql,
                        SqlUtils.makeItem("COUNT", 1));
                String zeroDateCondition = String.format("`%s` = '0000-00-00 00:00:00'", splitBy);
                zeroDateSql = SqlUtils.addWhere(zeroDateSql, zeroDateCondition);
                LOG.warn("The TIMESTAMP splitBy sql type may cause data-missing, check '0000-00-00 00:00:00' existence sql: " + zeroDateSql);
                long zeroDateNum = manager.executeSelect(zeroDateSql, 1, ResultSetGetter.LONG_GETTER).get(0);
                if (zeroDateNum > 0) {
                    LOG.warn("Zero date row num: " + nullRowNum);
                    // 如果存在，那么加一个split，思路同null，查一把再加而不是无脑加的考量在于，
                    // 如果有qps限制而无谓地加一个有可能为空的map，那么会有大量的qps额度被浪费
                    res.add(new RDBMSInputSplit(zeroDateCondition, zeroDateCondition));
                } else {
                    LOG.info("Zero date row num: " + nullRowNum);
                }
            }

            LOG.info(String.format("Actually split to %d splits: %s", res.size(), res.toString()));
            return new SplitResult(res, notNullRowNum + nullRowNum);
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            // More-or-less ignore SQL exceptions here, but log in case we need it.
            try {
                if (null != minMaxCountResult) {
                    minMaxCountResult.close();
                }
            } catch (SQLException se) {
                LOG.warn("SQLException closing resultset: " + se.toString());
            }

            try {
                if (null != minMaxCountStatement) {
                    minMaxCountStatement.close();
                }
            } catch (SQLException se) {
                LOG.warn("SQLException closing statement: " + se.toString());
            }

            try {
                if (null != connection) {
                    connection.close();
                }
            } catch (SQLException se) {
                LOG.warn("SQLException closing connection: " + se.toString());
            }
        }
    }

    public static class SplitResult {
        private List<InputSplit> inputSplitList;
        private long totalNum;

        public SplitResult(List<InputSplit> inputSplitList, long totalNum) {
            this.inputSplitList = inputSplitList;
            this.totalNum = totalNum;
        }

        public List<InputSplit> getInputSplitList() {
            return inputSplitList;
        }

        public long getTotalNum() {
            return totalNum;
        }
    }

}
