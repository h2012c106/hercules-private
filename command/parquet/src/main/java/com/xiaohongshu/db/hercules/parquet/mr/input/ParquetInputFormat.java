package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf.EMPTY_AS_NULL;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf.ORIGINAL_SPLIT;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetInputFormat extends HerculesInputFormat<GroupWithSchemaInfo> implements InjectedClass {

    private static final Log LOG = LogFactory.getLog(ParquetInputFormat.class);

    private final ExampleInputFormat delegate = new ExampleInputFormat();

    private SchemaStyle schemaStyle;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    @SuppressWarnings("unchecked")
    private List<FileStatus> listStatus(JobContext context) {
        try {
            Method listStatusMethod = FileInputFormat.class.getDeclaredMethod("listStatus", JobContext.class);
            listStatusMethod.setAccessible(true);
            return (List<FileStatus>) listStatusMethod.invoke(delegate, context);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new MapReduceException(e);
        }
    }

    private long getJobSize(List<FileStatus> stats) {
        long count = 0;
        for (FileStatus stat : stats) {
            count += stat.getLen();
        }
        return count;
    }

    /**
     * 根据总文件大小以及配置的mapper数量计算每个mapper split的处理的数据量大小
     * 如果task side metadata，那么这个数量还会受到文件数目的影响（>=文件数）
     */
    private void configureNumMapper(JobContext context, int numSplits) {
        List<FileStatus> stats = listStatus(context);
        long totalFileSize = getJobSize(stats);
        long splitSize = totalFileSize / numSplits;
        LOG.info(String.format("Total file num: %d, total file size (byte): %d, estimated per split size (byte): %d",
                stats.size(), totalFileSize, splitSize));

        // 以下注释无视file数对map数造成的影响，皆在单个file的情况下讨论

        // 定义了min size且大于block size后会导致一个split中有多个block，可能会产生较大网络开销，可能弊大于利
        // 处理map数小于block数的情况
        context.getConfiguration().setLong("mapred.max.split.size", splitSize);
        context.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", splitSize);

        // 定义了max size且小于block size后会导致split切分单个block
        // 处理map数大于block数的情况
        context.getConfiguration().setLong("mapred.max.split.size", splitSize);
        context.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
    }

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        if (!options.getBoolean(ORIGINAL_SPLIT, false)) {
            configureNumMapper(context, numSplits);
        }
        List<InputSplit> tmpRes = delegate.getSplits(context);
        if (tmpRes.size() > numSplits) {
            List<InputSplit> res = new ArrayList<>(numSplits);
            for (int i = 0; i < numSplits; ++i) {
                res.add(new ParquetCombinedInputSplit());
            }
            // 发牌
            int i = 0;
            for (InputSplit inputSplit : tmpRes) {
                ((ParquetCombinedInputSplit) res.get(i)).add(inputSplit);
                i = ++i < numSplits ? i : 0;
            }
            return res;
        } else if (tmpRes.size() < numSplits) {
            LOG.warn(String.format("Unable to split more than %d split(s).", tmpRes.size()));
            return tmpRes.stream().map(ParquetCombinedInputSplit::new).collect(Collectors.toList());
        } else {
            return tmpRes.stream().map(ParquetCombinedInputSplit::new).collect(Collectors.toList());
        }
    }

    @Override
    public void afterInject() {
        schemaStyle = SchemaStyle.valueOfIgnoreCase(options.getString(SCHEMA_STYLE, null));
    }

    @Override
    protected HerculesRecordReader<GroupWithSchemaInfo> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetRecordReader(context, delegate);
    }

    @Override
    protected ParquetInputWrapperManager createWrapperGetterFactory() {
        switch (schemaStyle) {
            case SQOOP:
                return new ParquetSqoopInputWrapperManager();
            case HIVE:
                return new ParquetHiveInputWrapperManager();
            case ORIGINAL:
                boolean emptyAsNull = options.getBoolean(EMPTY_AS_NULL, false);
                return new ParquetHerculesInputWrapperManager(emptyAsNull);
            default:
                throw new RuntimeException();
        }
    }
}
