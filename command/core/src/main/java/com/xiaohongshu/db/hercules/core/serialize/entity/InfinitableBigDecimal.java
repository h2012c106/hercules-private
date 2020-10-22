package com.xiaohongshu.db.hercules.core.serialize.entity;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;
import java.util.StringJoiner;

public class InfinitableBigDecimal implements Comparable<InfinitableBigDecimal> {

    private InfinityFlag flag;
    private BigDecimal value;

    public static InfinitableBigDecimal ZERO = InfinitableBigDecimal.valueOf(BigDecimal.ZERO);
    public static InfinitableBigDecimal ONE = InfinitableBigDecimal.valueOf(BigDecimal.ONE);

    private InfinitableBigDecimal() {
    }

    public static InfinitableBigDecimal valueOf(float value) {
        InfinitableBigDecimal res = new InfinitableBigDecimal();
        if (value == Float.NEGATIVE_INFINITY) {
            res.flag = InfinityFlag.NEGATIVE;
            res.value = BigDecimal.valueOf(Long.MIN_VALUE);
        } else if (value == Float.POSITIVE_INFINITY) {
            res.flag = InfinityFlag.POSITIVE;
            res.value = BigDecimal.valueOf(Long.MAX_VALUE);
        } else {
            res.flag = InfinityFlag.NORMAL;
            res.value = BigDecimal.valueOf(value);
        }
        return res;
    }

    public static InfinitableBigDecimal valueOf(double value) {
        InfinitableBigDecimal res = new InfinitableBigDecimal();
        if (value == Double.NEGATIVE_INFINITY) {
            res.flag = InfinityFlag.NEGATIVE;
            res.value = BigDecimal.valueOf(Long.MIN_VALUE);
        } else if (value == Double.POSITIVE_INFINITY) {
            res.flag = InfinityFlag.POSITIVE;
            res.value = BigDecimal.valueOf(Long.MAX_VALUE);
        } else {
            res.flag = InfinityFlag.NORMAL;
            res.value = BigDecimal.valueOf(value);
        }
        return res;
    }

    public static InfinitableBigDecimal valueOf(BigDecimal value) {
        InfinitableBigDecimal res = new InfinitableBigDecimal();
        res.flag = InfinityFlag.NORMAL;
        res.value = value;
        return res;
    }

    public InfinityFlag getFlag() {
        return flag;
    }

    public BigDecimal getDecimalValue() {
        return value;
    }

    public Float getFloatValue() {
        switch (flag) {
            case NORMAL:
                return OverflowUtils.numberToFloat(value);
            case NEGATIVE:
                return Float.NEGATIVE_INFINITY;
            case POSITIVE:
                return Float.POSITIVE_INFINITY;
            default:
                throw new RuntimeException("Unknown infinity flag: " + flag);
        }
    }

    public Double getDoubleValue() {
        switch (flag) {
            case NORMAL:
                return OverflowUtils.numberToDouble(value);
            case NEGATIVE:
                return Double.NEGATIVE_INFINITY;
            case POSITIVE:
                return Double.POSITIVE_INFINITY;
            default:
                throw new RuntimeException("Unknown infinity flag: " + flag);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InfinitableBigDecimal that = (InfinitableBigDecimal) o;
        return flag == that.flag &&
                Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(flag, value);
    }

    @Override
    public String toString() {
        switch (flag) {
            case NORMAL:
                return value.toPlainString();
            case NEGATIVE:
                return "-Inf";
            case POSITIVE:
                return "Inf";
            default:
                throw new RuntimeException("Unknown infinity flag: " + flag);
        }
    }

    @Override
    public int compareTo(InfinitableBigDecimal o) {
        if (getFlag().isFinite() && o.getFlag().isFinite()) {
            return getDecimalValue().compareTo(o.getDecimalValue());
        } else {
            return Integer.compare(getFlag().getCompareFlag(), o.getFlag().getCompareFlag());
        }
    }

    public enum InfinityFlag {
        NEGATIVE(-1),
        POSITIVE(1),
        NORMAL(0);

        private int compareFlag;

        InfinityFlag(int compareFlag) {
            this.compareFlag = compareFlag;
        }

        public int getCompareFlag() {
            return compareFlag;
        }

        public boolean isNegativeInfinity() {
            return this == NEGATIVE;
        }

        public boolean isPositiveInfinity() {
            return this == POSITIVE;
        }

        public boolean isFinite() {
            return this == NORMAL;
        }
    }
}
