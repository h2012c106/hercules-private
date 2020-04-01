package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exceptions.MapReduceException;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherPair;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public abstract class HerculesRecordReader<T, S extends BaseSchemaFetcher>
        extends RecordReader<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordReader.class);

    /**
     * 事先记录好每个下标对应的列如何转换到base wrapper的方法，不用每次读到一列就switch...case了
     */
    protected List<WrapperGetter<T>> wrapperGetterList;

    protected WrappingOptions options;

    protected S schemaFetcher;

    public HerculesRecordReader(S schemaFetcher) {
        this.schemaFetcher = schemaFetcher;
    }

    abstract protected void myInitialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    private <X> List<WrapperGetter<T>> makeWrapperGetterList(final BaseSchemaFetcher<X> schemaFetcher) {
        return schemaFetcher.getColumnNameList()
                .stream()
                .map(columnName -> getWrapperGetter(schemaFetcher.getColumnTypeMap().get(columnName))
                )
                .collect(Collectors.toList());
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        wrapperGetterList = makeWrapperGetterList(schemaFetcher);

        myInitialize(split, context);
    }

    private WrapperGetter<T> getWrapperGetter(@NonNull DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return getIntegerGetter();
            case DOUBLE:
                return getDoubleGetter();
            case BOOLEAN:
                return getBooleanGetter();
            case STRING:
                return getStringGetter();
            case DATE:
                return getDateGetter();
            case BYTES:
                return getBytesGetter();
            case NULL:
                return getNullGetter();
            default:
                throw new MapReduceException("Unknown data type: " + dataType.name());
        }
    }

    abstract protected WrapperGetter<T> getIntegerGetter();

    abstract protected WrapperGetter<T> getDoubleGetter();

    abstract protected WrapperGetter<T> getBooleanGetter();

    abstract protected WrapperGetter<T> getStringGetter();

    abstract protected WrapperGetter<T> getDateGetter();

    abstract protected WrapperGetter<T> getBytesGetter();

    abstract protected WrapperGetter<T> getNullGetter();
}
