package com.xiaohongshu.db.hercules.core.mr.output;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SerDerAssembly;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public abstract class HerculesOutputFormat<T> extends OutputFormat<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(HerculesOutputFormat.class);

    @GeneralAssembly(role = DataSourceRole.TARGET)
    private DataSource dataSource;

    @SerDerAssembly(role = DataSourceRole.SER)
    private KVSer<?> kvSer;

    public HerculesOutputFormat() {
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.TARGET;
    }

    protected void innerCheckOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public final void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 初始化context，因为hadoop的这三兄弟的构造都不归我管，且仅在进入这几个函数时我能获得configuration用以初始化context，
        // 但是这几个函数初次调用顺序并不一定，所以都做一次，context内部仅会做一次初始化
        HerculesContext.initialize(context.getConfiguration()).inject(this);

        innerCheckOutputSpecs(context);
    }

    protected OutputCommitter innerGetOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    @Override
    public final OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化context，因为hadoop的这三兄弟的构造都不归我管，且仅在进入这几个函数时我能获得configuration用以初始化context，
        // 但是这几个函数初次调用顺序并不一定，所以都做一次，context内部仅会做一次初始化
        HerculesContext.initialize(context.getConfiguration()).inject(this);

        return innerGetOutputCommitter(context);
    }

    abstract protected HerculesRecordWriter<T> innerGetRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException;

    @Override
    public final RecordWriter<NullWritable, HerculesWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        // 初始化context，因为hadoop的这三兄弟的构造都不归我管，且仅在进入这几个函数时我能获得configuration用以初始化context，
        // 但是这几个函数初次调用顺序并不一定，所以都做一次，context内部仅会做一次初始化
        HerculesContext.initialize(context.getConfiguration()).inject(this);

        HerculesRecordWriter<T> delegate = innerGetRecordWriter(context);

        RecordWriter<NullWritable, HerculesWritable> res;
        if (dataSource.hasKvSerDer() && kvSer != null) {
            HerculesContext.instance().inject(delegate);
            res = new HerculesSerDerRecordWriter(kvSer, delegate);
        } else {
            res = delegate;
        }
        HerculesContext.instance().inject(res);

        WrapperSetterFactory<T> wrapperSetterFactory = createWrapperSetterFactory();
        HerculesContext.instance().inject(wrapperSetterFactory);
        delegate.setWrapperSetterFactory(wrapperSetterFactory);
        delegate.afterSetWrapperSetterFactory();

        return res;
    }

    abstract protected WrapperSetterFactory<T> createWrapperSetterFactory();

}
