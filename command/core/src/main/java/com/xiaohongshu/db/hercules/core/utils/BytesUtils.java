package com.xiaohongshu.db.hercules.core.utils;

import com.sun.tools.javac.util.Assert;
import org.apache.commons.lang3.StringUtils;

public final class BytesUtils {

    private static final int BYTE_LENGTH = 8;

    /**
     * 当成补码
     */
    public static byte[] binStrToBytes(String binStr) {
        Assert.check(!StringUtils.isEmpty(binStr), "The bin string cannot be empty.");
        int binInt = Integer.parseInt(binStr, 2);
        int div = binStr.toCharArray().length / BYTE_LENGTH;
        int mod = binStr.toCharArray().length % BYTE_LENGTH;
        int byteLen = div;
        if (mod != 0) {
            ++byteLen;
        }
        byte[] res = new byte[byteLen];
        for (int i = 0; i < byteLen; ++i) {
            int rightMoveStep = byteLen - i - 1;
            res[i] = (byte) ((binInt >> (rightMoveStep * BYTE_LENGTH)) & 0xFF);
        }
        return res;
    }

    private static String fillByte(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < BYTE_LENGTH - s.length(); ++i) {
            sb.append("0");
        }
        return sb.append(s).toString();
    }

    /**
     * 写作补码
     */
    public static String bytesToBinStr(byte[] bytes) {
        Assert.check(bytes != null && bytes.length > 0, "The bytes cannot be empty.");
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            String binStr = Integer.toBinaryString((b & 0xFF));
            // 补齐头部的0
            sb.append(fillByte(binStr));
        }
        // 把头部的0trim掉
        return StringUtils.stripStart(sb.toString(), "0");
    }

}
