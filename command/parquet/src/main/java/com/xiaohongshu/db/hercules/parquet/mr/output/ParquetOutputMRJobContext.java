package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.context.BaseMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
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

import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN_TYPE;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.DIR;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf.COMPRESSION_CODEC;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOutputOptionsConf.DELETE_TARGET_DIR;

public class ParquetOutputMRJobContext extends BaseMRJobContext {

    private static final Log LOG = LogFactory.getLog(ParquetOutputMRJobContext.class);

    public ParquetOutputMRJobContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void configureJob(Job job) {
        String schema = getOptions().getString(MESSAGE_TYPE, null);
        if (StringUtils.isEmpty(schema)) {
            throw new MapReduceException(String.format("There must exist a parquet schema for writing. Use '--%s' or '--%s' to specify it.",
                    COLUMN_TYPE, MESSAGE_TYPE));
        }
        ExampleOutputFormat.setSchema(job, MessageTypeParser.parseMessageType(schema));

        CompressionCodecName compressionCodec
                = CompressionCodecName.fromConf(getOptions().getString(COMPRESSION_CODEC, null));
        ExampleOutputFormat.setCompression(job, compressionCodec);

        Path targetDir = new Path(getOptions().getString(DIR, null));
        // 清空目标目录
        if (getOptions().getBoolean(DELETE_TARGET_DIR, false)) {
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
    public void preRun() {
    }

    @Override
    public void postRun() {
    }
}
