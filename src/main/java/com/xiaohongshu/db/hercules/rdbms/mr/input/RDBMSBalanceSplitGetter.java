package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.rdbms.mr.input.splitter.BaseSplitter;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class RDBMSBalanceSplitGetter implements SplitGetter {

    private static final Log LOG = LogFactory.getLog(RDBMSBalanceSplitGetter.class);

    private static final BigDecimal MIN_MEANINGFUL_SAMPLE_NUM = BigDecimal.valueOf(30);
    private static final BigDecimal FULL_NUM_CONSIDERED_AS_SMALL = BigDecimal.valueOf(100);
    // 置信水平99%
    private static final BigDecimal Z = BigDecimal.valueOf(2.58);

    private BigDecimal getActualSampleSize(BigDecimal sampleSize, String splitBy, Map<String, DataType> columnTypeMap,
                                           BigDecimal maxSampleRow) {
        int singleByteSize;
        DataType dataType = columnTypeMap.get(splitBy);
        if (!dataType.isCustom()) {
            switch ((BaseDataType) dataType) {
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case DECIMAL:
                    singleByteSize = 8;
                    break;
                case BOOLEAN:
                    singleByteSize = 1;
                    break;
                case DATE:
                case TIME:
                case DATETIME:
                    singleByteSize = 16;
                    break;
                case STRING:
                    singleByteSize = 128;
                    LOG.warn(String.format("The string split-by column may cause oom when use balance mode, " +
                            "if happened, try '--%s'", RDBMSInputOptionsConf.BALANCE_SPLIT_SAMPLE_MAX_ROW));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }

        BigDecimal availableMemoryByte = BigDecimal.valueOf(Runtime.getRuntime().maxMemory()
                - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory())
                .divide(BigDecimal.TEN, BigDecimal.ROUND_DOWN);
        // 取内存允许量(十分之一)以及当前大小的较小值
        BigDecimal res = sampleSize
                .min(availableMemoryByte.divide(BigDecimal.valueOf(singleByteSize), 8, BigDecimal.ROUND_UP));
        if (maxSampleRow != null) {
            res = res.min(maxSampleRow);
        }
        return res;
    }

    private BigDecimal calculateSampleRatio(BigDecimal sampleSize, BigDecimal fullSize) {
        // 乘以一个系数用于尽量避免少抽，精确的算出若想要在99%或95%以上的概率从fullSize抽出sampleSize个是不可能的
        // 设需要至少从n个里抽出s个的概率>=r%, 需要解如下关于x的方程:
        // || s-1                         ||
        // ||  Σ  xˢ*(1-x)ⁿ⁻ˢ*C(n,s) < r% ||
        // || k=0                         ||
        // 解出来的x即答案，用python脚本循环逼近结果后发现在总数较小时x的取值基本为实际比例的两倍左右（当然这个比例随着抽样数增大逐渐减小），
        // 随着总数增大，到1000时，已经基本等于实际比例，再大就算不动组合数了。
        // 另外，这里有个需要注意的点，假设当总数为10，抽样率为0.6时，实际取到5个和7个的概率是不相等的，是一个右倾的图样，并不可以等效为正态分布
        if (fullSize.compareTo(FULL_NUM_CONSIDERED_AS_SMALL) <= 0) {
            sampleSize = sampleSize.multiply(BigDecimal.valueOf(2));
        } else {
            sampleSize = sampleSize.multiply(BigDecimal.valueOf(1.2));
        }
        return sampleSize.divide(fullSize, 9, BigDecimal.ROUND_UP).min(BigDecimal.ONE);
    }

    private List sample(RDBMSManager manager, String splitBy,
                        BigDecimal sampleSize, BigDecimal fullSize, BaseSplitter splitter,
                        BigDecimal maxSampleRow, Map<String, DataType> columnTypeMap,
                        String baseSql) throws SQLException {
        sampleSize = getActualSampleSize(sampleSize, splitBy, columnTypeMap, maxSampleRow);
        BigDecimal sampleRatio = calculateSampleRatio(sampleSize, fullSize);

        String rawSql = baseSql;
        rawSql = SqlUtils.replaceSelectItem(rawSql, splitBy);
        rawSql = SqlUtils.addWhere(rawSql, String.format("%s < %s",
                manager.getRandomFunc(), sampleRatio.toPlainString()));
        rawSql = SqlUtils.addNullCondition(rawSql, splitBy, false);
        LOG.info("The sampling sql is: " + rawSql);
        List sampledResult = manager.executeSelect(rawSql, 1, splitter.getResultSetGetter());
        LOG.info(String.format("Intended sampled size vs Actual sampled size: %s vs %d",
                sampleSize.toPlainString(), sampledResult.size()));
        return sampledResult;
    }

    @Override
    public List<InputSplit> getSplits(ResultSet minMaxCountResult, int numSplits, String splitBy,
                                      Map<String, DataType> columnTypeMap, String baseSql, BaseSplitter splitter,
                                      BigDecimal maxSampleRow, RDBMSManager manager) throws SQLException {
        BigDecimal nullRowCount = BigDecimal.valueOf(minMaxCountResult.getLong(3));
        BigDecimal sampleVariance = splitter.getVariance(sample(manager, splitBy,
                MIN_MEANINGFUL_SAMPLE_NUM, nullRowCount, splitter, maxSampleRow, columnTypeMap, baseSql));

        BigDecimal allowableError = splitter.getGap().divide(BigDecimal.valueOf(numSplits).pow(2),
                8, BigDecimal.ROUND_UP);
        BigDecimal sampleSize = Z.pow(2)
                .multiply(sampleVariance)
                .divide(allowableError.pow(2), 8, BigDecimal.ROUND_UP)
                .max(MIN_MEANINGFUL_SAMPLE_NUM);

        return splitter.getBalanceSplitPoint(splitBy,
                sample(manager, splitBy, sampleSize, nullRowCount, splitter, maxSampleRow, columnTypeMap, baseSql),
                numSplits);
    }
}
