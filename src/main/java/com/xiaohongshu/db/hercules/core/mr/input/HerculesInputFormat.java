package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverterInitializer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
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
        implements DataTypeConverterInitializer<C> {

    private static final Log LOG = LogFactory.getLog(HerculesInputFormat.class);

    public HerculesInputFormat() {
    }

    protected C converter;
    protected Map<String, DataType> columnTypeMap;

    protected void initializeContext(GenericOptions sourceOptions) {
        converter = initializeConverter();
        columnTypeMap = SchemaUtils.convert(sourceOptions.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));
    }

    abstract protected List<InputSplit> innerGetSplits(JobContext context) throws IOException, InterruptedException;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        initializeContext(options.getSourceOptions());

        List<InputSplit> res = innerGetSplits(context);

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
    public HerculesRecordReader<?, C> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        System.out.println("##############\n");
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        initializeContext(options.getSourceOptions());

        return innerCreateRecordReader(split, context);
    }

    abstract protected HerculesRecordReader<?, C> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;
}
