package com.xiaohongshu.db.hercules.bson.mr;

import org.apache.commons.lang3.StringUtils;

public enum CompressionCodecName {
    NONE(""),
    SNAPPY("org.apache.hadoop.io.compress.SnappyCodec"),
    GZIP("org.apache.hadoop.io.compress.GzipCodec"),
    //    LZO("com.hadoop.compression.lzo.LzoCodec"),
//    BROTLI("org.apache.hadoop.io.compress.BrotliCodec"),
//    ZSTD("hercules.shaded.org.apache.hadoop.io.compress.ZStandardCodec"),
    LZ4("org.apache.hadoop.io.compress.Lz4Codec");

    private final String hadoopCompressionCodecClass;

    public static CompressionCodecName valueOfIgnoreCase(String value) {
        for (CompressionCodecName compressionCodecName : CompressionCodecName.values()) {
            if (StringUtils.equalsIgnoreCase(compressionCodecName.name(), value)) {
                return compressionCodecName;
            }
        }
        throw new RuntimeException("Illegal CompressionCodecName type: " + value);
    }

    public String getHadoopCompressionCodecClass() {
        return hadoopCompressionCodecClass;
    }

    CompressionCodecName(String hadoopCompressionCodecClass) {
        this.hadoopCompressionCodecClass = hadoopCompressionCodecClass;
    }
}
