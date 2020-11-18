package com.xiaohongshu.db.hercules.core.utils;

import lombok.NonNull;

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

    public static final BigDecimal MIN_FLOAT_POSITIVE = new BigDecimal(
            String.valueOf(Float.MIN_VALUE));

    public static final BigDecimal MAX_FLOAT_POSITIVE = new BigDecimal(
            String.valueOf(Float.MAX_VALUE));

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

    private static boolean isFloatOverFlow(final BigDecimal decimal) {
        if (decimal.signum() == 0) {
            return false;
        }

        BigDecimal newDecimal = decimal;
        boolean isPositive = decimal.signum() == 1;
        if (!isPositive) {
            newDecimal = decimal.negate();
        }

        return (newDecimal.compareTo(MIN_FLOAT_POSITIVE) < 0 || newDecimal
                .compareTo(MAX_FLOAT_POSITIVE) > 0);
    }

    public static byte numberToByte(@NonNull Number number) {
        return new BigDecimal(number.toString()).toBigInteger().byteValueExact();
    }

    public static short numberToShort(@NonNull Number number) {
        return new BigDecimal(number.toString()).toBigInteger().shortValueExact();
    }

    public static int numberToInteger(@NonNull Number number) {
        return new BigDecimal(number.toString()).toBigInteger().intValueExact();
    }

    public static long numberToLong(@NonNull Number number) {
        return new BigDecimal(number.toString()).toBigInteger().longValueExact();
    }

    public static float numberToFloat(@NonNull BigDecimal number) {
        float res = number.floatValue();
        if (isFloatOverFlow(number)) {
            throw new ArithmeticException("Overflow float value: " + number.toString());
        }
        return res;
    }

    public static float numberToFloat(@NonNull Number number) {
        return numberToFloat(new BigDecimal(number.toString()));
    }

    public static double numberToDouble(@NonNull BigDecimal number) {
        double res = number.doubleValue();
        if (isDoubleOverFlow(number)) {
            throw new ArithmeticException("Overflow double value: " + number.toString());
        }
        return res;
    }

    public static double numberToDouble(@NonNull Number number) {
        return numberToDouble(new BigDecimal(number.toString()));
    }
}