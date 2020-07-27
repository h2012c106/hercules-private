package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN_TYPE;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.DIR;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf.COMPRESSION_CODEC;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf.DELETE_TARGET_DIR;

public class ParquetOutputMRJobContext implements MRJobContext {

    private static final Log LOG = LogFactory.getLog(ParquetOutputMRJobContext.class);

    @Override
    public void configureJob(Job job, WrappingOptions options) {
        String schema = options.getTargetOptions().getString(MESSAGE_TYPE, null);
        if (StringUtils.isEmpty(schema)) {
            throw new MapReduceException(String.format("There must exist a parquet schema for writing. Use '--%s' or '--%s' to specify it.",
                    COLUMN_TYPE, MESSAGE_TYPE));
        }
        ExampleOutputFormat.setSchema(job, MessageTypeParser.parseMessageType(schema));

        CompressionCodecName compressionCodec
                = CompressionCodecName.fromConf(options.getTargetOptions().getString(COMPRESSION_CODEC, null));
        ExampleOutputFormat.setCompression(job, compressionCodec);

        Path targetDir = new Path(options.getTargetOptions().getString(DIR, null));
        // 清空目标目录
        if (options.getTargetOptions().getBoolean(DELETE_TARGET_DIR, false)) {
            try {
                FileSystem fs = targetDir.getFileSystem(job.getConfiguration());

                if (fs.exists(targetDir)) {
                    fs.delete(targetDir, true);
                    LOG.info("Destination directory " + targetDir + " deleted.");
                } else {
                    LOG.info("Destination directory " + targetDir + " is not present, "
                            + "hence not deleting.");
                }
            } catch (IOException e) {
                LOG.error("Delete target dir failed.");
                throw new RuntimeException(e);
            }
        }
        ExampleOutputFormat.setOutputPath(job, targetDir);
    }

    @Override
    public void preRun(WrappingOptions options) {
    }

    @Override
    public void postRun(WrappingOptions options) {
    }
}
