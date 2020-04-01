package com.xiaohongshu.db.hercules.rdbms.input.mr.splitter;

import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TextSplitter extends BaseSplitter<String> {

    private boolean nVarchar;
    private String commonPrefix;

    public TextSplitter(boolean nVarchar) {
        this.nVarchar = nVarchar;
    }

    @Override
    public ResultSetGetter<String> getResultSetGetter() {
        return new ResultSetGetter<String>() {
            @Override
            public String get(ResultSet resultSet, int seq) throws SQLException {
                return resultSet.getString(seq);
            }
        };
    }

    /**
     * 以下一段抄自sqoop
     */

    private static final BigDecimal ONE_PLACE = new BigDecimal(65536);
    // Maximum number of characters to convert. This is to prevent rounding
    // errors or repeating fractions near the very bottom from getting out of
    // control. Note that this still gives us a huge number of possible splits.
    private static final int MAX_CHARS = 8;


    /**
     * Return a BigDecimal representation of string 'str' suitable for use in a
     * numerically-sorting order.
     */
    private BigDecimal stringToBigDecimal(String str) {
        // Start with 1/65536 to compute the first digit.
        BigDecimal curPlace = ONE_PLACE;
        BigDecimal result = BigDecimal.ZERO;

        int len = Math.min(str.length(), MAX_CHARS);

        for (int i = 0; i < len; i++) {
            int codePoint = str.codePointAt(i);
            result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
            // advance to the next less significant place. e.g., 1/(65536^2) for the
            // second char.
            curPlace = curPlace.multiply(ONE_PLACE);
        }

        return result;
    }

    /**
     * Return the string encoded in a BigDecimal.
     * Repeatedly multiply the input value by 65536; the integer portion after
     * such a multiplication represents a single character in base 65536.
     * Convert that back into a char and create a string out of these until we
     * have no data left.
     */
    private String bigDecimalToString(BigDecimal bd) {
        BigDecimal cur = bd.stripTrailingZeros();
        StringBuilder sb = new StringBuilder();

        for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
            cur = cur.multiply(ONE_PLACE);
            int curCodePoint = cur.intValue();
            if (0 == curCodePoint) {
                break;
            }

            cur = cur.subtract(new BigDecimal(curCodePoint));
            sb.append(Character.toChars(curCodePoint));
        }

        return sb.toString();
    }

    @Override
    protected void setCommonPrefix(String minVal, String maxVal) {
        // If there is a common prefix between minString and maxString, establish
        // it and pull it out of minString and maxString.
        int maxPrefixLen = Math.min(minVal.length(), maxVal.length());
        int sharedLen;
        for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
            char c1 = minVal.charAt(sharedLen);
            char c2 = maxVal.charAt(sharedLen);
            if (c1 != c2) {
                break;
            }
        }

        // The common prefix has length 'sharedLen'. Extract it from both.
        commonPrefix = minVal.substring(0, sharedLen);
    }

    @Override
    protected BigDecimal convertToDecimal(String value) {
        return stringToBigDecimal(value.substring(commonPrefix.length()));
    }

    @Override
    protected String convertFromDecimal(BigDecimal value) {
        String res = commonPrefix + bigDecimalToString(value);

        // 抄自mysql jdbc preparedStatement setString方法
        StringBuilder buf = new StringBuilder((int) ((double) res.length() * 1.1D));
        for (int i = 0; i < res.length(); ++i) {
            char c = res.charAt(i);
            switch (c) {
                case '\u0000':
                    buf.append('\\');
                    buf.append('0');
                    break;
                case '\n':
                    buf.append('\\');
                    buf.append('n');
                    break;
                case '\r':
                    buf.append('\\');
                    buf.append('r');
                    break;
                case '\u001a':
                    buf.append('\\');
                    buf.append('Z');
                    break;
                case '"':
                    buf.append('\\');
                    buf.append('"');
                    break;
                case '\'':
                    buf.append('\\');
                    buf.append('\'');
                    break;
                case '\\':
                    buf.append('\\');
                    buf.append('\\');
                    break;
                default:
                    buf.append(c);
            }
        }
        return buf.toString();
    }

    @Override
    protected String[] quote() {
        return nVarchar ? new String[]{"N'", "'"} : new String[]{"'", "'"};
    }
}
