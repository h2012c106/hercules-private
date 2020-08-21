package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KVDer;
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

public abstract class HerculesInputFormat<T> extends InputFormat<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(HerculesInputFormat.class);

    private static final String MAP_NUM_LIMIT_PROPERTY = "mapreduce.job.running.map.limit";

    public HerculesInputFormat() {
    }

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private DataSource dataSource;

    @SerDerAssembly(role = DataSourceRole.DER)
    private KVDer<?> kvDer;

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.SOURCE;
    }

    abstract protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException;

    @Override
    public final List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        // 初始化context，因为hadoop的这三兄弟的构造都不归我管，且仅在进入这几个函数时我能获得configuration用以初始化context，
        // 但是这几个函数初次调用顺序并不一定，所以都做一次，context内部仅会做一次初始化
        HerculesContext.initialize(context.getConfiguration()).inject(this);

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
            double parallelNum;
            if (context.getConfiguration().get(MAP_NUM_LIMIT_PROPERTY, null) == null) {
                parallelNum = actualNumSplits;
            } else {
                parallelNum = context.getConfiguration().getLong(MAP_NUM_LIMIT_PROPERTY, Long.MAX_VALUE);
            }
            double maxWriteQpsPerMap = maxWriteQps / parallelNum;
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
    public final RecordReader<NullWritable, HerculesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // 初始化context，因为hadoop的这三兄弟的构造都不归我管，且仅在进入这几个函数时我能获得configuration用以初始化context，
        // 但是这几个函数初次调用顺序并不一定，所以都做一次，context内部仅会做一次初始化
        HerculesContext.initialize(context.getConfiguration()).inject(this);

        HerculesRecordReader<T> delegate = innerCreateRecordReader(split, context);

        RecordReader<NullWritable, HerculesWritable> res;
        if (dataSource.hasKvSerDer() && kvDer != null) {
            HerculesContext.instance().inject(delegate);
            res = new HerculesSerDerRecordReader(kvDer, delegate);
        } else {
            res = delegate;
        }
        HerculesContext.instance().inject(res);

        WrapperGetterFactory<T> wrapperGetterFactory = createWrapperGetterFactory();
        HerculesContext.instance().inject(wrapperGetterFactory);
        delegate.setWrapperGetterFactory(wrapperGetterFactory);
        delegate.afterSetWrapperGetterFactory();

        return res;
    }

    abstract protected HerculesRecordReader<T> innerCreateRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    abstract protected WrapperGetterFactory<T> createWrapperGetterFactory();
}
