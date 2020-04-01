package com.xiaohongshu.db.hercules.core.utils;

import com.xiaohongshu.db.hercules.core.exceptions.SerializeException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 抄的DataX
 */
public final class OverflowUtils {
    public static final BigInteger MAX_LONG = BigInteger
            .valueOf(Long.MAX_VALUE);

    public static final BigInteger MIN_LONG = BigInteger
            .valueOf(Long.MIN_VALUE);

    public static final BigDecimal MIN_DOUBLE_POSITIVE = new BigDecimal(
            String.valueOf(Double.MIN_VALUE));

    public static final BigDecimal MAX_DOUBLE_POSITIVE = new BigDecimal(
            String.valueOf(Double.MAX_VALUE));

    private static boolean isLongOverflow(final BigInteger integer) {
        return (integer.compareTo(MAX_LONG) > 0 || integer
                .compareTo(MIN_LONG) < 0);

    }

    public static void validateLong(final BigInteger integer) {
        boolean isOverFlow = isLongOverflow(integer);

        if (isOverFlow) {
            throw new SerializeException("Unable to convert to long due to overflow: " + integer.toString());
        }
    }

    private static boolean isDoubleOverFlow(final BigDecimal decimal) {
        if (decimal.signum() == 0) {
            return false;
        }

        BigDecimal newDecimal = decimal;
        boolean isPositive = decimal.signum() == 1;
        if (!isPositive) {
            newDecimal = decimal.negate();
        }

        return (newDecimal.compareTo(MIN_DOUBLE_POSITIVE) < 0 || newDecimal
                .compareTo(MAX_DOUBLE_POSITIVE) > 0);
    }

    public static void validateDouble(final BigDecimal decimal) {
        boolean isOverFlow = isDoubleOverFlow(decimal);
        if (isOverFlow) {
            throw new SerializeException("Unable to convert to double due to overflow: " + decimal.toString());
        }
    }
}