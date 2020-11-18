package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import lombok.SneakyThrows;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;
import java.util.concurrent.*;

public class ParquetAsyncRecordReader extends ParquetRecordReader {

    private final ExecutorService pool = new ThreadPoolExecutor(
            2,
            2,
            0,
            TimeUnit.DAYS,
            new SynchronousQueue<>(),
            new RejectedExecutionHandler() {
                @Override
                @SneakyThrows
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    executor.getQueue().put(r);
                }
            }
    );
    private boolean first = true;
    private boolean next;

    public ParquetAsyncRecordReader(TaskAttemptContext context, ExampleInputFormat delegateInputFormat) {
        super(context, delegateInputFormat);
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        if (first) {
            first = false;
            return myNextKeyValue();
        } else {
            return next;
        }
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        try {
            final Group delegateCurrentValue = delegate.getCurrentValue();
            Future<HerculesWritable> valueFuture = pool.submit(new Callable<HerculesWritable>() {
                @Override
                public HerculesWritable call() throws Exception {
                    return new HerculesWritable(((ParquetInputWrapperManager) wrapperGetterFactory).groupToMapWrapper(delegateCurrentValue, null));
                }
            });
            Future<Boolean> nextFuture = pool.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return myNextKeyValue();
                }
            });
            next = nextFuture.get();
            return valueFuture.get();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void innerClose() throws IOException {
        super.innerClose();
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
