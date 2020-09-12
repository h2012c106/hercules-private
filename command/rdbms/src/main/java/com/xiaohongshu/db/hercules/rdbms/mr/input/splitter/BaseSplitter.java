package com.xiaohongshu.db.hercules.rdbms.mr.input.splitter;

import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputSplit;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseSplitter<T extends Comparable<T>> {

    private static final Log LOG = LogFactory.getLog(BaseSplitter.class);

    private T minVal;
    private T maxVal;
    private long totalSize;

    public BaseSplitter(ResultSet minMaxCountResult) throws SQLException {
        minVal = getResult(minMaxCountResult, 1);
        maxVal = getResult(minMaxCountResult, 2);
        totalSize = minMaxCountResult.getLong(3);

        setCommonPrefix(minVal, maxVal);
    }

    abstract public ResultSetGetter<T> getResultSetGetter();

    /**
     * 子类实现getLong等方法
     *
     * @param resultSet
     * @param i
     * @return
     */
    protected T getResult(ResultSet resultSet, int i) throws SQLException {
        return getResultSetGetter().get(resultSet, i);
    }


    /**
     * 在不考虑切到重复值的前提下，一个{@param rowNum}长度的列表切成{@param numSplits}份的理想切分点下标
     *
     * @param rowNum
     * @param numSplits
     * @return
     */
    private List<Integer> getRawPointsSeq(long rowNum, int numSplits) {
        LinkedHashSet<Integer> tempSet = new LinkedHashSet<>();
        for (int i = 0; i <= numSplits; ++i) {
            tempSet.add((int) Math.round((double) i * (double) (rowNum - 1) / (double) numSplits));
        }
        return new ArrayList<>(tempSet);
    }

    public BigDecimal getGap() {
        return convertToDecimal(maxVal).subtract(convertToDecimal(minVal));
    }


    public BigDecimal getVariance(List<T> sampledResult) {
        if (sampledResult.size() == 0) {
            sampledResult.add(minVal);
            sampledResult.add(maxVal);
        }
        List<BigDecimal> convertedList = sampledResult.stream()
                .map(this::convertToDecimal)
                .collect(Collectors.toList());
        BigDecimal size = BigDecimal.valueOf(convertedList.size());
        if (size.equals(BigDecimal.ONE) || size.equals(BigDecimal.ZERO)) {
            return BigDecimal.ZERO;
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal item : convertedList) {
            sum = sum.add(item);
        }
        BigDecimal avg = sum.divide(size, 8, BigDecimal.ROUND_UP);
        BigDecimal molecule = BigDecimal.ZERO;
        for (BigDecimal item : convertedList) {
            molecule = molecule.add(item.subtract(avg).pow(2));
        }
        return molecule.divide(size.subtract(BigDecimal.ONE), 8, BigDecimal.ROUND_UP);
    }

    public List<InputSplit> getBalanceSplitPoint(String splitBy, List<T> sampledResult, int numSplits) {
        // 千万不可去重，去重了数据分布就不一致了
        sampledResult = sampledResult.stream().sorted().collect(Collectors.toList());

        LOG.info(String.format("The sampling size is: %d, actual sample ratio is: %f",
                sampledResult.size(),
                ((double) sampledResult.size()) / ((double) totalSize)));

        sampledResult.add(0, minVal);
        sampledResult.add(maxVal);

        List<T> splitPoints = new ArrayList<>();
        // 上一个切分点的值
        T last = null;
        // 第n个切分点
        int n = 0;
        // 第t个抽样列表数值
        int m = 0;
        List<Integer> pointsSeqList = getRawPointsSeq(sampledResult.size(), numSplits);
        while (m < sampledResult.size()) {
            // 这个切分点与上一个切分点值相同，则往下走一位，直到走到最后一次查询的数组的最后一位
            while (m < sampledResult.size() && sampledResult.get(m).equals(last)) {
                ++m;
            }
            // 如果找不同找到了当前数组的末尾
            if (m == sampledResult.size()) {
                // 把当前最后一位加入splitPoints
                m -= 1;
            }
            last = sampledResult.get(m);
            splitPoints.add(last);
            // 有可能m自增的次数超过了一段split的长度，如果只是简单的n++，有可能m会取到更小的值
            while (n < pointsSeqList.size() && m >= pointsSeqList.get(n)) {
                ++n;
            }
            if (n == pointsSeqList.size()) {
                break;
            }
            m = pointsSeqList.get(n);
        }
        // 这里做去重+排序其实是多此一举了
        return generateInputSplitList(splitBy, splitPoints);
    }

    /**
     * 等比转换，至少大小关系一定要保持一致
     *
     * @param value
     * @return
     */
    abstract protected BigDecimal convertToDecimal(T value);

    /**
     * 等比转换，至少大小关系一定要保持一致
     *
     * @param value
     * @return
     */
    abstract protected T convertFromDecimal(BigDecimal value);

    private static final BigDecimal MIN_INCREMENT =
            new BigDecimal(10000 * Double.MIN_VALUE);

    protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
        try {
            return numerator.divide(denominator);
        } catch (ArithmeticException ae) {
            // 由于ROUND_UP，不可能取到0值
            return numerator.divide(denominator, BigDecimal.ROUND_UP);
        }
    }

    /**
     * 用于对text型提出公共前缀的方法，对其他类型直接略过
     *
     * @param minVal
     * @param maxVal
     */
    protected void setCommonPrefix(T minVal, T maxVal) {
    }

    public List<InputSplit> getFastSplitPoint(String splitBy, int numSplits) {

        List<BigDecimal> splits = new ArrayList<BigDecimal>();

        BigDecimal decimalMinVal = convertToDecimal(minVal);
        BigDecimal decimalMaxVal = convertToDecimal(maxVal);
        BigDecimal decimalNumSplits = BigDecimal.valueOf(numSplits);

        BigDecimal splitSize = tryDivide(decimalMaxVal.subtract(decimalMinVal), (decimalNumSplits));
        if (splitSize.compareTo(MIN_INCREMENT) < 0) {
            splitSize = MIN_INCREMENT;
            LOG.warn("Set BigDecimal splitSize to MIN_INCREMENT: " + MIN_INCREMENT.toPlainString());
        }

        BigDecimal curVal = decimalMinVal;

        // min值不可能大于max值，所以数组里至少有一个min
        while (curVal.compareTo(decimalMaxVal) <= 0) {
            splits.add(curVal);
            curVal = curVal.add(splitSize);
        }

        // 转换回来
        List<T> res = splits
                .stream()
                .map(this::convertFromDecimal)
                .collect(Collectors.toList());
        // 确保不漏
        res.add(0, minVal);
        res.add(maxVal);
        // 去掉界外值+去重+排序
        res = res.stream()
                .filter(item -> item.compareTo(minVal) >= 0 && item.compareTo(maxVal) <= 0)
                .distinct()
                .sorted()
                .collect(Collectors.toList());
        return generateInputSplitList(splitBy, res);
    }

    /**
     * @return #0是左符号，#1是右符号
     */
    protected Enclosing quote() {
        return new Enclosing("", "");
    }

    private String quotedValue(String value) {
        Enclosing enclosing = quote();
        return enclosing.getLeft() + value + enclosing.getRight();
    }

    static public List<InputSplit> generateNullSplit(String columnName) {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        splits.add(new RDBMSInputSplit(columnName + " IS NULL", columnName + " IS NULL"));
        return splits;
    }

    static public List<InputSplit> generateAllSplit() {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        splits.add(new RDBMSInputSplit("1 = 1", "1 = 1"));
        return splits;
    }

    public List<InputSplit> generateInputSplitList(String columnName, List<T> splitPoints) {
        String lowClausePrefix = columnName + " >= ";
        String highClausePrefix = columnName + " < ";
        String endClausePrefix = columnName + " <= ";

        List<InputSplit> splits = new ArrayList<InputSplit>();

        // Turn the split points into a set of intervals.
        T start = splitPoints.get(0);
        for (int i = 1; i < splitPoints.size(); i++) {
            T end = splitPoints.get(i);

            if (i == splitPoints.size() - 1) {
                // This is the last one; use a closed interval.
                splits.add(new RDBMSInputSplit(
                        lowClausePrefix + quotedValue(start.toString()),
                        endClausePrefix + quotedValue(end.toString())));
            } else {
                // Normal open-interval case.
                splits.add(new RDBMSInputSplit(
                        lowClausePrefix + quotedValue(start.toString()),
                        highClausePrefix + quotedValue(end.toString())));
            }

            start = end;
        }

        return splits;
    }

    public static class Enclosing {
        private String left;
        private String right;

        public Enclosing(String left, String right) {
            this.left = left;
            this.right = right;
        }

        public String getLeft() {
            return left;
        }

        public String getRight() {
            return right;
        }
    }

}
