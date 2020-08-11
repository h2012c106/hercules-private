package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SerDerAssembly;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public abstract class HerculesInputFormat<T> extends InputFormat<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesInputFormat.class);

    public HerculesInputFormat() {
        HerculesContext.instance().inject(this);
    }

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private DataSource dataSource;

    @SerDerAssembly(role = DataSourceRole.SOURCE)
    private KvSerDer<?, ?> kvSerDer;

    abstract protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        int numSplits = commonOptions.getInteger(CommonOptionsConf.NUM_MAPPER,
                CommonOptionsConf.DEFAULT_NUM_MAPPER);

        List<InputSplit> res = innerGetSplits(context, numSplits);

        long actualNumSplits = res.size();
        if (actualNumSplits < numSplits) {
            LOG.warn(String.format("Actual map size is less than configured: %d vs %d", actualNumSplits, numSplits));
        } else if (actualNumSplits > numSplits) {
            LOG.warn(String.format("Actual map size is more than configured: %d vs %d", actualNumSplits, numSplits));
        }

        // 换算各个mapper实际的qps
        if (commonOptions.hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            double maxWriteQps = commonOptions.getDouble(CommonOptionsConf.MAX_WRITE_QPS, null);
            double maxWriteQpsPerMap = maxWriteQps / (double) actualNumSplits;
            LOG.info("Max write qps per map is: " + maxWriteQpsPerMap);
            // 在这里设置options吊用没有，这里的和别的地方的是深拷贝关系，要设置Configuration
            // options.getCommonOptions().set(CommonOptionsConf.MAX_WRITE_QPS, maxWriteQpsPerMap);
            context.getConfiguration().setDouble(
                    GenericOptions.getConfigurationName(CommonOptionsConf.MAX_WRITE_QPS, OptionsType.COMMON),
                    maxWriteQpsPerMap
            );
        }

        return res;
    }

    @Override
    public RecordReader<NullWritable, HerculesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        HerculesRecordReader<T> delegate = innerCreateRecordReader(split, context);
        WrapperGetterFactory<T> wrapperGetterFactory = createWrapperGetterFactory();
        HerculesContext.instance().inject(wrapperGetterFactory);
        delegate.setWrapperGetterFactory(wrapperGetterFactory);

        RecordReader<NullWritable, HerculesWritable> res;
        if (dataSource.hasKvSerDer() && kvSerDer != null) {
            res = new HerculesSerDerRecordReader(kvSerDer, delegate);
        } else {
            res = delegate;
        }
        HerculesContext.instance().inject(res);
        return res;
    }

    abstract protected HerculesRecordReader<T> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    abstract protected WrapperGetterFactory<T> createWrapperGetterFactory();
}
