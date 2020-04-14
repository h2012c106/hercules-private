package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSBalanceSplitGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.input.SplitGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.input.splitter.BaseSplitter;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClickhouseBalanceSplitGetter implements SplitGetter {

    private static final Log LOG = LogFactory.getLog(ClickhouseBalanceSplitGetter.class);

    private String makeQuantileSql(String querySql, int numSplits, String splitBy) {
        List<BigDecimal> quantilePointList = new LinkedList<>();
        BigDecimal decimalNumSplits = BigDecimal.valueOf(numSplits);
        for (BigDecimal i = BigDecimal.ONE; i.compareTo(decimalNumSplits) < 0; i = i.add(BigDecimal.ONE)) {
            quantilePointList.add(i.divide(decimalNumSplits, 8, BigDecimal.ROUND_HALF_EVEN));
        }
        quantilePointList.add(0, BigDecimal.ZERO);
        quantilePointList.add(BigDecimal.ONE);
        String quantilePointListStr = quantilePointList.stream()
                .filter(item -> item.compareTo(BigDecimal.ONE) < 0 && item.compareTo(BigDecimal.ZERO) > 0)
                .map(BigDecimal::toPlainString)
                .collect(Collectors.joining(", ", "quatiles(", ")"));
        String res = SqlUtils.replaceSelectItem(querySql,
                SqlUtils.makeItem(quantilePointListStr, splitBy));
        res = SqlUtils.addNullCondition(res, splitBy, false);
        return res;
    }

    @Override
    public List<InputSplit> getSplits(ResultSet minMaxCountResult, int numSplits,
                                      String splitBy, RDBMSSchemaFetcher schemaFetcher,
                                      BaseSplitter splitter, BigDecimal maxSampleRow) throws SQLException {
        String quantileSql = makeQuantileSql(schemaFetcher.getQuerySql(), numSplits, splitBy);
        LOG.info("Quantile sql is: " + quantileSql);
        try {
            Object[] quantileResult = (Object[]) schemaFetcher.getManager().executeSelect(quantileSql, 1, new ResultSetGetter<Array>() {
                @Override
                public Array get(ResultSet resultSet, int seq) throws SQLException {
                    return resultSet.getArray(seq);
                }
            }).get(0).getArray();
            return splitter.generateInputSplitList(splitBy, Arrays.asList(quantileResult));
        } catch (SQLException e) {
            String errorMessage = e.getMessage();
            String regex = "^.*Illegal type (.+?) of argument for aggregate function quantiles$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(errorMessage);
            if (matcher.find()) {
                LOG.warn(String.format("The type '%s' cannot be applied to clickhouse 'quatiles' method, " +
                        "use sample balance method instead, exception message is: %s", matcher.group(1), errorMessage));
                // 降级成普通sample balance
                return new RDBMSBalanceSplitGetter().getSplits(minMaxCountResult, numSplits, splitBy, schemaFetcher, splitter, maxSampleRow);
            } else {
                throw e;
            }
        }
    }
}
