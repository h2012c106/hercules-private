package com.xiaohongshu.db.hercules.parquet;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import com.xiaohongshu.db.hercules.parquet.schema.TypeBuilderTreeNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public final class ParquetUtils {

    private final static int DAY_MILLISEC = 24 * 60 * 60 * 1000;

    public final static String STRING_ENCODE = "UTF-8";

    public final static int DEFAULT_PRECISION = 38;
    public final static int DEFAULT_SCALE = 12;

    public static Type getNestedColumnType(String fullColumnName, MessageType messageType) {
        if (StringUtils.isEmpty(fullColumnName)) {
            return messageType;
        }
        WritableUtils.ColumnSplitResult splitResult = WritableUtils.splitColumnWrapped(fullColumnName);
        GroupType tmpGroupType = messageType;
        for (String columnName : splitResult.getParentColumnList()) {
            tmpGroupType = tmpGroupType.getType(columnName).asGroupType();
        }
        return tmpGroupType.getType(splitResult.getFinalColumn());
    }

    public static BigDecimal bytesToDecimal(Binary binary, int scale) {
        // 解铃召唤系铃人
        return new HiveDecimalWritable(binary.getBytes(), scale).getHiveDecimal().bigDecimalValue();
    }

    /**
     * 完全照抄hive的private方法
     */
    private static Binary decimalToBinary(final HiveDecimal hiveDecimal, final DecimalTypeInfo decimalTypeInfo) {
        int prec = decimalTypeInfo.precision();
        int scale = decimalTypeInfo.scale();

        byte[] decimalBytes = hiveDecimal.bigIntegerBytesScaled(scale);

        // Estimated number of bytes needed.
        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }

        // Padding leading zeroes/ones.
        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length);
        return Binary.fromByteArray(tgt);
    }

    public static Binary decimalToBytes(BigDecimal decimal, int precision, int scale) {
        return decimalToBinary(HiveDecimal.create(decimal), new DecimalTypeInfo(precision, scale));
    }

    public static Date intToDate(int dayAfter19700101) {
        // 不能这么来，拿出来的天数一定是本地时区下的从0点开始算的0点到下一天0点之前的一天，
        // 而这么算的话如果是+8那么一天是从8点开始的，肯定和存hive初衷不一致
        // 由于hive存的是差值，不带时区信息，那么我们就需要加上时区信息
        // 相当于这一列就时区无关了
        // return new Date(TimeUnit.DAYS.toMillis(dayAfter19700101));

        org.apache.hadoop.hive.common.type.Date date = org.apache.hadoop.hive.common.type.Date.ofEpochDay(dayAfter19700101);
        return new Date(date.getYear() - 1900, date.getMonth() - 1, date.getDay());
    }

    public static int dateToInt(Date date) {
        // 这样有一个问题，如果上游给一个+8的1970-01-02 01:00:00，由于时区的缘故，还算做第一天，此函数会返回0，也就是会显示成1970-01-01
        // 不能这么来，拿出来的天数一定是本地时区下的从0点开始算的0点到下一天0点之前的一天，
        // 而这么算的话如果是+8那么一天是从8点开始的，肯定和存hive初衷不一致
        // 由于hive存的是差值，不带时区信息，那么我们就需要加上时区信息
        // 相当于这一列就时区无关了
        // return OverflowUtils.numberToInteger(TimeUnit.MILLISECONDS.toDays(date.getTime()));

        return org.apache.hadoop.hive.common.type.Date
                .of(date.getYear() + 1900, date.getMonth() + 1, date.getDate())
                .toEpochDay();
    }

    public static Date bytesToDatetime(Binary bytes) {
        NanoTime nanoTime = NanoTime.fromBinary(bytes);
        Timestamp timestamp = NanoTimeUtils.getTimestamp(nanoTime, false);
        return timestamp.toSqlTimestamp();
    }

    public static Binary datetimeToBytes(Date date) {
        Timestamp timestamp = Timestamp.ofEpochMilli(date.getTime());
        // 如果hive那侧发现读出来少8个小时，试试把hive.parquet.timestamp.skip.conversion置false，
        // 这样就会按照当前时区读出值，不然是按照UTC读
        return NanoTimeUtils.getNanoTime(timestamp, false).toBinary();
    }

    public static BigInteger longlongToBigInteger(Binary binary) {
        // 还没想好
        throw new UnsupportedOperationException();
    }

    public static Binary bigIntegerToLonglong(BigInteger integer) {
        // 还没想好
        throw new UnsupportedOperationException();
    }

    public static Date intToTime(int millisecAfter19700101) {
        millisecAfter19700101 %= DAY_MILLISEC;
        // 相当于在Hercules有时区，存储时无时区信息
        return new Date(millisecAfter19700101);
    }

    public static int timeToInt(Date date) {
        long res = date.getTime();
        res %= DAY_MILLISEC;
        // 相当于在Hercules有时区，存储时无时区信息
        return OverflowUtils.numberToInteger(res);
    }

    public static Date longToDatetime(long millisecAfter19700101) {
        // 相当于在Hercules有时区，存储时无时区信息
        return new Date(millisecAfter19700101);
    }

    public static long datetimeToLong(Date date) {
        // 相当于在Hercules有时区，存储时无时区信息
        return date.getTime();
    }

}
