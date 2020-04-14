package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.SchemaFetcherGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherPair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public abstract class HerculesInputFormat<S extends BaseSchemaFetcher>
        extends InputFormat<NullWritable, HerculesWritable>
        implements SchemaFetcherGetter<S> {

    private static final Log LOG = LogFactory.getLog(HerculesInputFormat.class);

    public HerculesInputFormat() {
    }

    abstract protected List<InputSplit> innerGetSplits(JobContext context) throws IOException, InterruptedException;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

        List<InputSplit> res = innerGetSplits(context);

        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        // 换算各个mapper实际的qps
        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            double maxWriteQps = options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null);
            double numMapper = res.size();
            double maxWriteQpsPerMap = maxWriteQps / numMapper;
            LOG.info("Max write qps per map is: " + maxWriteQpsPerMap);
            options.getCommonOptions().set(CommonOptionsConf.MAX_WRITE_QPS, maxWriteQpsPerMap);
        }

        return res;
    }

    @Override
    abstract public HerculesRecordReader<?, S> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException;

    abstract protected S innerGetSchemaFetcher(GenericOptions options);

    @Override
    public final S getSchemaFetcher(GenericOptions options) {
        S res = innerGetSchemaFetcher(options);
        SchemaFetcherPair.set(res, DataSourceRole.SOURCE);
        return res;
    }
}
