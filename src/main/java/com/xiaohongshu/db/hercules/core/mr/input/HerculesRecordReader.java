package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @param <T> 数据源读入时用于表示一行的数据结构，详情可见{@link WrapperGetter}
 */
public abstract class HerculesRecordReader<T, C extends DataTypeConverter>
        extends RecordReader<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordReader.class);

    private WrapperGetterFactory<T> wrapperGetterFactory;

    protected WrappingOptions options;

    protected List<String> columnNameList;
    protected Map<String, DataType> columnTypeMap;

    protected C converter;

    protected boolean emptyColumnNameList;

    public HerculesRecordReader(C converter, WrapperGetterFactory<T> wrapperGetterFactory) {
        this.converter = converter;
        this.wrapperGetterFactory = wrapperGetterFactory;
    }

    /**
     * 子类有可能会有内部非静态类（如mongo），初始化无法new出来，留个口子
     *
     * @param wrapperGetterFactory
     */
    protected void setWrapperGetterFactory(WrapperGetterFactory<T> wrapperGetterFactory) {
        this.wrapperGetterFactory = wrapperGetterFactory;
    }

    abstract protected void myInitialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        // 虽然negotiator中会强制塞，但是空值似乎传不过来
        columnNameList = Arrays.asList(options.getSourceOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        columnTypeMap = SchemaUtils.convert(options.getSourceOptions().getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));

        emptyColumnNameList = columnNameList.size() == 0;

        myInitialize(split, context);
    }

    protected final WrapperGetter<T> getWrapperGetter(DataType dataType) {
        return wrapperGetterFactory.getWrapperGetter(dataType);
    }

}
