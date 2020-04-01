package com.xiaohongshu.db.hercules.core.mr.output;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.RateLimiter;
import com.xiaohongshu.db.hercules.common.options.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exceptions.MapReduceException;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class HerculesRecordWriter<T, S extends BaseSchemaFetcher> extends RecordWriter<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordWriter.class);

    protected WrappingOptions options;
    protected List<WrapperSetter<T>> wrapperSetterList;

    /**
     * 上游没有的列，这里会置null
     */
    protected String[] columnNames;
    protected List<Integer> targetSourceColumnSeq;

    protected S schemaFetcher;

    protected List<String> sourceColumnList;

    protected RateLimiter rateLimiter = null;

    protected <X> List<WrapperSetter<T>> makeWrapperSetterList(final BaseSchemaFetcher<X> schemaFetcher, List<String> columnNameList) {
        return columnNameList
                .stream()
                .map(columnName -> getWrapperSetter(schemaFetcher.getColumnTypeMap().get(columnName))
                )
                .collect(Collectors.toList());
    }

    protected List<String> filterExtraColumns(List<String> columnNameList, List<Integer> sourceColumnSeqList) {
        // columnNameList与sourceColumnSeqList一定长度相等
        List<String> res = new ArrayList<>(columnNameList.size());
        for (int i = 0; i < columnNameList.size(); ++i) {
            if (sourceColumnSeqList.get(i) != null) {
                res.add(columnNameList.get(i));
            } else {
                res.add(null);
            }
        }
        return res;
    }

    public HerculesRecordWriter(TaskAttemptContext context, S schemaFetcher) {
        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        this.schemaFetcher = schemaFetcher;

        columnNames = options.getTargetOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null);

        sourceColumnList = Arrays.asList(options.getSourceOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        List<String> targetColumnList = Arrays.asList(columnNames);
        JSONObject columnMap = options.getCommonOptions().getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject());

        wrapperSetterList = makeWrapperSetterList(schemaFetcher, targetColumnList);

        // 过滤上游没有的列，数据库会以default插入
        // 第一步，生成目标列list的各个列对应的源列的下标，若是目标表多的列，值为null
        targetSourceColumnSeq = SchemaUtils.mapColumnSeq(sourceColumnList, targetColumnList, columnMap);
        // 根据带null的targetSourceColumnSeq将对应列的name置null
        columnNames = filterExtraColumns(targetColumnList, targetSourceColumnSeq).toArray(new String[0]);

        LOG.info("The upstream column seq in downstream column order: " + targetSourceColumnSeq);

        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            rateLimiter = RateLimiter.create(options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null));
        }
    }

    private WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return getIntegerSetter();
            case DOUBLE:
                return getDoubleSetter();
            case BOOLEAN:
                return getBooleanSetter();
            case STRING:
                return getStringSetter();
            case DATE:
                return getDateSetter();
            case BYTES:
                return getBytesSetter();
            case NULL:
                return getNullSetter();
            default:
                throw new MapReduceException("Unknown data type: " + dataType.name());
        }
    }

    abstract protected void innerWrite(NullWritable key, HerculesWritable value) throws IOException, InterruptedException;

    /**
     * 即使下游是攒着批量写也没问题，在写之前一定等够了对应qps的时间，batch写一定避免不了毛刺的qps，但是batch间的qps是一定能保证的
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        innerWrite(key, value);
    }

    abstract protected WrapperSetter<T> getIntegerSetter();

    abstract protected WrapperSetter<T> getDoubleSetter();

    abstract protected WrapperSetter<T> getBooleanSetter();

    abstract protected WrapperSetter<T> getStringSetter();

    abstract protected WrapperSetter<T> getDateSetter();

    abstract protected WrapperSetter<T> getBytesSetter();

    abstract protected WrapperSetter<T> getNullSetter();

}
