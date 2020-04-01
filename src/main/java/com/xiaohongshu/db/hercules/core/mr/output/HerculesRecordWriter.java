package com.xiaohongshu.db.hercules.core.mr.output;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.common.options.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exceptions.MapReduceException;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.ArrayList;
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

    protected <X> List<WrapperSetter<T>> makeWrapperSetterList(final BaseSchemaFetcher<X> schemaFetcher, List<String> columnNameList) {
        return columnNameList
                .stream()
                .map(columnName -> getWrapperSetter(schemaFetcher.getColumnTypeMap().get(columnName))
                )
                .collect(Collectors.toList());
    }

    private <X> List<WrapperSetter<T>> makeWrapperSetterList(final BaseSchemaFetcher<X> schemaFetcher) {
        return makeWrapperSetterList(schemaFetcher, schemaFetcher.getColumnNameList());
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

        wrapperSetterList = makeWrapperSetterList(schemaFetcher);

        BaseSchemaFetcher targetSchemaFetcher = schemaFetcher;

        List<String> targetColumnList = targetSchemaFetcher.getColumnNameList();
        JSONObject columnMap = options.getCommonOptions().getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject());

        // 过滤上游没有的列，数据库会以default插入
        // 第一步，生成目标列list的各个列对应的源列的下标，若是目标表多的列，值为null
        targetSourceColumnSeq = SchemaUtils.mapColumnSeq(columnMap);
        // 根据带null的targetSourceColumnSeq将对应列的name置null
        columnNames = filterExtraColumns(targetColumnList, targetSourceColumnSeq).toArray(new String[0]);

        LOG.info("The upstream column seq in downstream column order: " + targetSourceColumnSeq);
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

    abstract protected WrapperSetter<T> getIntegerSetter();

    abstract protected WrapperSetter<T> getDoubleSetter();

    abstract protected WrapperSetter<T> getBooleanSetter();

    abstract protected WrapperSetter<T> getStringSetter();

    abstract protected WrapperSetter<T> getDateSetter();

    abstract protected WrapperSetter<T> getBytesSetter();

    abstract protected WrapperSetter<T> getNullSetter();

}
