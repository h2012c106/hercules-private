package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.SchemaFetcherGetter;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherPair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public abstract class HerculesInputFormat<S extends BaseSchemaFetcher>
        extends InputFormat<NullWritable, HerculesWritable>
        implements SchemaFetcherGetter<S> {

    private static final Log LOG = LogFactory.getLog(HerculesInputFormat.class);

    public HerculesInputFormat() {
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
