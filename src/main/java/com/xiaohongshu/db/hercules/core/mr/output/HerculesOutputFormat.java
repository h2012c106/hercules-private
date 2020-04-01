package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.SchemaFetcherGetter;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherPair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public abstract class HerculesOutputFormat<S extends BaseSchemaFetcher>
        extends OutputFormat<NullWritable, HerculesWritable>
        implements SchemaFetcherGetter<S> {

    private static final Log LOG = LogFactory.getLog(HerculesOutputFormat.class);

    public HerculesOutputFormat() {
    }

    @Override
    abstract public HerculesRecordWriter<?, S> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException;


    abstract protected S innerGetSchemaFetcher(GenericOptions options);

    @Override
    public final S getSchemaFetcher(GenericOptions options) {
        S res = innerGetSchemaFetcher(options);
        SchemaFetcherPair.set(res, DataSourceRole.TARGET);
        return res;
    }
}
