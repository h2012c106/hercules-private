package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseTableInputFormat extends TableInputFormat {

    private static final Log LOG = LogFactory.getLog(HBaseTableInputFormat.class);
    private GenericOptions sourceOptions;
    private HBaseManager manager;
    private DataTypeConverter converter;

    public HBaseTableInputFormat(HBaseManager manager, HBaseDataTypeConverter converter) {
        this.manager = manager;
        this.converter = converter;
    }

    @Override
    protected void initialize(JobContext context) throws IOException {

        Configuration configuration = context.getConfiguration();
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);
        sourceOptions = options.getSourceOptions();
        setConf(manager.getConf());
        super.initialize(context);
    }

    // An important step to setup database connection, make sure each conf option is correctly defined
    @SneakyThrows
    @Override
    public void setConf(Configuration conf) {

        manager.setSourceConf(conf, sourceOptions, manager);
        super.setConf(conf);
    }

    @Override
    public RecordReader createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException {

        RecordReader<ImmutableBytesWritable, Result> result =
                super.createRecordReader(split, context);

        HBaseRecordReader recordReader = new HBaseRecordReader(result, converter, sourceOptions.getString(HBaseInputOptionsConf.ROW_KEY_COL_NAME, null));

        return recordReader;
    }
}

class HBaseRecordReader extends HerculesRecordReader<NavigableMap<Long, byte[]>, DataTypeConverter> {


    private RecordReader<ImmutableBytesWritable, Result> result;
    private String rowKeyCol = null;

    public HBaseRecordReader(RecordReader result, DataTypeConverter converter, String rowKeyCol) {

        super(converter);
        this.result = result;
        if(rowKeyCol!=null){
            this.rowKeyCol = rowKeyCol;
        }
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        result.initialize(split, context);
    }

    @Override
    protected WrapperGetter getIntegerGetter() {
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> columnValueMap, String name, int seq) throws Exception {
                byte[] res = columnValueMap.firstEntry().getValue();
                if (res==null) {
                    return new NullWrapper();
                } else {
                    return new IntegerWrapper(Bytes.toInt(res));
                }
            }
        };
    }

    @Override
    protected WrapperGetter getDoubleGetter() {
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> columnValueMap, String name, int seq) throws Exception {
                byte[] res = columnValueMap.firstEntry().getValue();
                if (res==null) {
                    return new NullWrapper();
                } else {
                    return new DoubleWrapper(Bytes.toDouble(res));
                }
            }
        };
    }

    @Override
    protected WrapperGetter getBooleanGetter() {
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> columnValueMap, String name, int seq) throws Exception {
                byte[] res = columnValueMap.firstEntry().getValue();
                if (res==null) {
                    return new NullWrapper();
                } else {
                    return new BooleanWrapper(Bytes.toBoolean(res));
                }
            }
        };
    }

    @Override
    protected WrapperGetter getStringGetter() {
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> columnValueMap, String name, int seq) throws Exception {
                byte[] res = columnValueMap.firstEntry().getValue();
                if (res==null) {
                    return new NullWrapper();
                } else {
                    return new StringWrapper(Bytes.toString(res));
                }
            }
        };
    }

    @Override
    protected WrapperGetter getDateGetter() {
        return null;
    }

    @Override
    protected WrapperGetter getBytesGetter() {
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> columnValueMap, String name, int seq) throws Exception {
                byte[] res = columnValueMap.firstEntry().getValue();
                if (res==null) {
                    return new NullWrapper();
                } else {
                    return new BytesWrapper(res);
                }
            }
        };
    }

    @Override
    protected WrapperGetter getNullGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String name, int seq) throws Exception {
                return NullWrapper.INSTANCE;
            }
        };
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return result.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @SneakyThrows
    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getCurrentValue().getMap();

        int columnNum = columnNameList.size();
        HerculesWritable value = new HerculesWritable(columnNum);

        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for(int i=0;i<columnNum;i++){
                String columnName = (String) columnNameList.get(i);
                // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游
                if(rowKeyCol!=null){
                    value.put(rowKeyCol, (BaseWrapper) getWrapperGetter(DataType.STRING));
                }
                NavigableMap<Long, byte[]> columnValueMap = familyMap.get(columnName.getBytes());

                for (Map.Entry<Long, byte[]> s : columnValueMap.entrySet()) {                //获取列对应的不同版本数据，默认最新的一个
                    value.put(columnName, wrapperGetterList.get(i).get(columnValueMap, columnName, 0));
                }
            }
        }
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        result.close();
    }
}