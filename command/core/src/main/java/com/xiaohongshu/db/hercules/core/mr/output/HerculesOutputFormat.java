package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public abstract class HerculesOutputFormat<T> extends OutputFormat<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesOutputFormat.class);

    public HerculesOutputFormat() {
    }

    abstract protected HerculesRecordWriter<T> innerGetRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException;

    @Override
    public final RecordWriter<NullWritable, HerculesWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        HerculesRecordWriter<T> res = innerGetRecordWriter(context);
        res.setWrapperSetterFactory(createWrapperSetterFactory());
        if (HerculesContext.getAssemblySupplierPair().getTargetItem().getDataSource().hasKvSerializer()
                && HerculesContext.getKvSerializerSupplierPair().getTargetItem() != null) {
            return new HerculesSerializerRecordWriter(
                    HerculesContext.getKvSerializerSupplierPair().getTargetItem().getKvSerializer(),
                    res
            );
        } else {
            return res;
        }
    }

    abstract protected WrapperSetterFactory<T> createWrapperSetterFactory();

}
