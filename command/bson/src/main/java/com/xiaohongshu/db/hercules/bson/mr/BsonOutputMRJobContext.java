package com.xiaohongshu.db.hercules.bson.mr;

import com.xiaohongshu.db.hercules.bson.option.BsonOptionsConf;
import com.xiaohongshu.db.hercules.bson.option.BsonOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.mr.context.BaseMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BsonOutputMRJobContext extends BaseMRJobContext {

    private static final Log LOG = LogFactory.getLog(BsonOutputMRJobContext.class);

    public BsonOutputMRJobContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void configureJob(Job job) {
        Path targetDir = new Path(getOptions().getString(BsonOptionsConf.DIR, null));
        // 清空目标目录
        if (getOptions().getBoolean(BsonOutputOptionsConf.DELETE_TARGET_DIR, false)) {
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
        try {
            targetDir = targetDir.getFileSystem(job.getConfiguration()).makeQualified(
                    targetDir);
        } catch (IOException e) {
            // Throw the IOException as a RuntimeException to be compatible with MR1
            throw new RuntimeException(e);
        }
        job.getConfiguration().set(FileOutputFormat.OUTDIR, targetDir.toString());
        String compressCodec = getOptions().getString(BsonOutputOptionsConf.COMPRESS_CODEC, "snappy");
        CompressionCodecName codecName = CompressionCodecName.valueOfIgnoreCase(compressCodec);
        boolean toCompress = codecName != CompressionCodecName.NONE;
        job.getConfiguration().setBoolean(FileOutputFormat.COMPRESS, toCompress);
        if (toCompress) {
            job.getConfiguration().set(FileOutputFormat.COMPRESS_CODEC, codecName.getHadoopCompressionCodecClass());
            LOG.info("Codec: " + codecName.getHadoopCompressionCodecClass());
        }
    }

    @Override
    public void preRun() {
    }

    @Override
    public void postRun() {
    }
}
