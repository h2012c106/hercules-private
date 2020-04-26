package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
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
import java.util.List;
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

        conf.set(HBaseInputOptionsConf.INPUT_TABLE, sourceOptions.getString(HBaseInputOptionsConf.INPUT_TABLE, null));
        // input table name must be specified
//        if(conf.get(HbaseInputOptionsConf.INPUT_TABLE)==null){
//            throw new Exception("Input table name must be specified");
//        }
        conf.setBoolean(HBaseInputOptionsConf.MAPREDUCE_INPUT_AUTOBALANCE, sourceOptions.getBoolean(HBaseInputOptionsConf.MAPREDUCE_INPUT_AUTOBALANCE, true));
        conf.setInt(HBaseInputOptionsConf.NUM_MAPPERS_PER_REGION, sourceOptions.getInteger(HBaseInputOptionsConf.NUM_MAPPERS_PER_REGION, 1));

        conf.setBoolean(HBaseInputOptionsConf.SCAN_CACHEBLOCKS, sourceOptions.getBoolean(HBaseInputOptionsConf.SCAN_CACHEBLOCKS, false));
        conf.set(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY, sourceOptions.getString(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY, null));
        conf.setInt(HBaseInputOptionsConf.SCAN_CACHEDROWS, sourceOptions.getInteger(HBaseInputOptionsConf.INPUT_TABLE, 500));

//        conf.set(HBaseInputOptionsConf.MAX_AVERAGE_REGION_SIZE, sourceOptions.getString(HBaseInputOptionsConf.MAX_AVERAGE_REGION_SIZE, null));
        //if starStop key not specified, the start key and the stop key of the table will be collected.
        List<String> startStopKeys = manager.getTableStartStopKeys(conf.get(HBaseInputOptionsConf.INPUT_TABLE));
        conf.set(HBaseInputOptionsConf.SCAN_ROW_START, sourceOptions.getString(HBaseInputOptionsConf.SCAN_ROW_START, startStopKeys.get(0)));
        conf.set(HBaseInputOptionsConf.SCAN_ROW_STOP, sourceOptions.getString(HBaseInputOptionsConf.SCAN_ROW_STOP, startStopKeys.get(1)));

        // set timestamp for Scan
        conf.set(HBaseInputOptionsConf.SCAN_TIMERANGE_START,
                sourceOptions.getString(HBaseInputOptionsConf.SCAN_TIMERANGE_START, null));
        conf.set(HBaseInputOptionsConf.SCAN_TIMERANGE_END, sourceOptions.getString(HBaseInputOptionsConf.SCAN_TIMERANGE_END, null));
        conf.set(HBaseInputOptionsConf.SCAN_TIMESTAMP, sourceOptions.getString(HBaseInputOptionsConf.SCAN_TIMESTAMP, null));
        super.setConf(conf);
    }

    @Override
    public RecordReader createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException {

        RecordReader<ImmutableBytesWritable, Result> result =
                super.createRecordReader(split, context);

        HBaseRecordReader recordReader = new HBaseRecordReader(result, converter);

        return recordReader;
    }
}

class HBaseRecordReader extends HerculesRecordReader {


    private RecordReader<ImmutableBytesWritable, Result> result;
    protected List<WrapperGetter<NavigableMap<Long, byte[]>>> wrapperGetterList;

    public HBaseRecordReader(RecordReader result, DataTypeConverter converter) {

        super(converter);
        this.result = result;
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
    public Object getCurrentKey() {
        return NullWritable.get();
    }

    @SneakyThrows
    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getCurrentValue().getMap();

        HerculesWritable res = new HerculesWritable();
        // TODO 根据框架，获得 HerculesWritable 并返回。WrapperGetterList -> HerculesWritable

        int columnNum = columnNameList.size();
        HerculesWritable value = new HerculesWritable(columnNum);
        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for(int i=0;i<columnNum;i++){

                String columnName = (String) columnNameList.get(i);
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