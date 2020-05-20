package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.parser.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterGenerator;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
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
import java.util.Map;

public abstract class HerculesInputFormat<C extends DataTypeConverter>
        extends InputFormat<NullWritable, HerculesWritable>
        implements DataTypeConverterGenerator<C> {

    private static final Log LOG = LogFactory.getLog(HerculesInputFormat.class);

    public HerculesInputFormat() {
    }

    protected C converter;
    protected Map<String, DataType> columnTypeMap;
    protected GenericOptions options;

    protected void initializeContext(GenericOptions sourceOptions) {
        options = sourceOptions;
        converter = generateConverter();
        columnTypeMap = SchemaUtils.convert(sourceOptions.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));
    }

    abstract protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        initializeContext(options.getSourceOptions());

        int numSplits = options.getCommonOptions().getInteger(CommonOptionsConf.NUM_MAPPER,
                CommonOptionsConf.DEFAULT_NUM_MAPPER);

        List<InputSplit> res = innerGetSplits(context, numSplits);

        long actualNumSplits = res.size();
        if (actualNumSplits < numSplits) {
            LOG.warn(String.format("Actual map size is less than configured: %d vs %d", actualNumSplits, numSplits));
        } else if (actualNumSplits > numSplits) {
            LOG.warn(String.format("Actual map size is more than configured: %d vs %d", actualNumSplits, numSplits));
        }

        // 换算各个mapper实际的qps
        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            double maxWriteQps = options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null);
            double maxWriteQpsPerMap = maxWriteQps / (double) actualNumSplits;
            LOG.info("Max write qps per map is: " + maxWriteQpsPerMap);
            // 在这里设置options吊用没有，这里的和别的地方的是深拷贝关系，要设置Configuration
            // options.getCommonOptions().set(CommonOptionsConf.MAX_WRITE_QPS, maxWriteQpsPerMap);
            configuration.setDouble(GenericOptions.getConfigurationName(CommonOptionsConf.MAX_WRITE_QPS, OptionsType.COMMON),
                    maxWriteQpsPerMap);
        }

        return res;
    }

    @Override
    public HerculesRecordReader<?, C> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        initializeContext(options.getSourceOptions());

        return innerCreateRecordReader(split, context);
    }

    abstract protected HerculesRecordReader<?, C> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;
}
