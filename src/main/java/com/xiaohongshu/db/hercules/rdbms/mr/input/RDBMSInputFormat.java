package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.rdbms.mr.input.splitter.*;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSDataTypeConverter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManagerInitializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

public class RDBMSInputFormat extends HerculesInputFormat<RDBMSDataTypeConverter> implements RDBMSManagerInitializer {

    private static final Log LOG = LogFactory.getLog(RDBMSInputFormat.class);

    public static final String AVERAGE_MAP_ROW_NUM = "hercules.average.map.row.num";

    private RDBMSManager manager;
    private String baseSql;
    private RDBMSSchemaFetcher schemaFetcher;

    /**
     * rdbms使用{@link StingyMap}，不允许（不可能）拿到未被fetch到类型的列。
     */
    private StingyMap<String, DataType> columnTypeMap;

    protected RDBMSSchemaFetcher initializeSchemaFetcher(GenericOptions options,
                                      RDBMSDataTypeConverter converter,
                                      RDBMSManager manager){
        return new RDBMSSchemaFetcher(options, converter, manager);
    }

    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        super.initializeContext(sourceOptions);

        manager = initializeManager(sourceOptions);
        schemaFetcher = initializeSchemaFetcher(sourceOptions, converter, manager);
        baseSql = SqlUtils.makeBaseQuery(sourceOptions);

        columnTypeMap = new StingyMap<>(super.columnTypeMap);
    }

    private BaseSplitter getSplitter(ResultSet minMaxCountResult,
                                     DataType dataType, int sqlDataType, boolean hexString) throws SQLException {
        switch (dataType) {
            case INTEGER:
                return new IntegerSplitter(minMaxCountResult);
            case DOUBLE:
                return new DoubleSplitter(minMaxCountResult);
            case BOOLEAN:
                return new BooleanSplitter(minMaxCountResult);
            case DATE:
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
                throw new UnsupportedOperationException("Unsupported data type to split: " + dataType.name());
        }
    }

    protected SplitGetter getSplitGetter(GenericOptions options) {
        if (options.getBoolean(RDBMSInputOptionsConf.BALANCE_SPLIT, true)) {
            return new RDBMSBalanceSplitGetter();
        } else {
            return new RDBMSFastSplitterGetter();
        }
    }

    @Override
    public List<InputSplit> innerGetSplits(JobContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        initializeContext(options.getSourceOptions());

        // 检查split-by列在不在列集里
        String splitBy;
        try {
            splitBy = options.getSourceOptions().getString(RDBMSInputOptionsConf.SPLIT_BY, null);
            if (splitBy == null) {
                splitBy = schemaFetcher.getPrimaryKey();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        if (splitBy == null) {
            throw new SchemaException(String.format("Cannot get the split-by column automatically, " +
                    "please use '--%s' to specify.", RDBMSInputOptionsConf.SPLIT_BY));
        }

        // 如果只有一个split，那下面的不用做了
        int numSplits = options.getCommonOptions().getInteger(CommonOptionsConf.NUM_MAPPER,
                CommonOptionsConf.DEFAULT_NUM_MAPPER);
        if (numSplits == 1) {
            LOG.warn("Map set to 1, only use 1 map.");
            return BaseSplitter.generateAllSplit();
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
                    columnTypeMap.get(splitBy),
                    sqlDataType,
                    options.getSourceOptions().getBoolean(RDBMSInputOptionsConf.SPLIT_BY_HEX_STRING, false)
            );

            // 如果没有不null的行，那么直接返回一个split
            long notNullRowNum = minMaxCountResult.getLong(3);
            LOG.info("Not null row num: " + notNullRowNum);
            if (notNullRowNum == 0) {
                LOG.warn("Full of null! Use 1 map.");
                return BaseSplitter.generateNullSplit(splitBy);
            }

            String nullSql = SqlUtils.replaceSelectItem(baseSql,
                    SqlUtils.makeItem("COUNT", 1));
            nullSql = SqlUtils.addNullCondition(nullSql, splitBy, true);
            LOG.info("The count null sql is: " + nullSql);

            long nullRowNum = manager.executeSelect(nullSql, 1, ResultSetGetter.LONG_GETTER).get(0);
            LOG.info("Null row num: " + nullRowNum);

            BigDecimal maxSampleRow = options.getSourceOptions()
                    .getDecimal(RDBMSInputOptionsConf.BALANCE_SPLIT_SAMPLE_MAX_ROW, null);

            List<InputSplit> res = getSplitGetter(options.getSourceOptions())
                    .getSplits(minMaxCountResult, numSplits, splitBy, columnTypeMap, baseSql, splitter, maxSampleRow, manager);

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

            configuration.setLong(AVERAGE_MAP_ROW_NUM, (notNullRowNum + nullRowNum) / numSplits);

            LOG.info(String.format("Actually split to %d splits: %s", res.size(), res.toString()));
            return res;
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            // More-or-less ignore SQL exceptions here, but log in case we need it.
            try {
                if (null != minMaxCountResult) {
                    minMaxCountResult.close();
                }
            } catch (SQLException se) {
                LOG.debug("SQLException closing resultset: " + se.toString());
            }

            try {
                if (null != minMaxCountStatement) {
                    minMaxCountStatement.close();
                }
            } catch (SQLException se) {
                LOG.debug("SQLException closing statement: " + se.toString());
            }

            try {
                if (null != connection) {
                    connection.close();
                }
            } catch (SQLException se) {
                LOG.debug("SQLException closing connection: " + se.toString());
            }
        }
    }

    @Override
    public HerculesRecordReader<ResultSet, RDBMSDataTypeConverter> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        initializeContext(options.getSourceOptions());

        return new RDBMSRecordReader(manager, initializeConverter());
    }

    @Override
    public RDBMSManager initializeManager(GenericOptions options) {
        return new RDBMSManager(options);
    }

    @Override
    public RDBMSDataTypeConverter initializeConverter() {
        return new RDBMSDataTypeConverter();
    }
}
