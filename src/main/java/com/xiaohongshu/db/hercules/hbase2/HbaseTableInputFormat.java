package com.xiaohongshu.db.hercules.hbase2;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.hbase2.MangerFactory.ManagerFactory;
import com.xiaohongshu.db.hercules.hbase2.option.HbaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase2.schema.manager.HbaseManager;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class HbaseTableInputFormat extends TableInputFormat {


    private GenericOptions sourceOptions;
    private HbaseManager manager;

    @Override
    protected void initialize(JobContext context) throws IOException {

        Configuration configuration = context.getConfiguration();
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);
        sourceOptions = options.getSourceOptions();
        manager = ManagerFactory.getManager(sourceOptions, HbaseManager.class);
        setConf(manager.getConf());
        super.initialize(context);
    }

    // An important step to setup database connection, make sure each conf option is correctly defined
    @SneakyThrows
    @Override
    public void setConf(Configuration conf) {

        conf.set(HbaseInputOptionsConf.INPUT_TABLE, sourceOptions.getString(HbaseInputOptionsConf.INPUT_TABLE, null));
        // input table name must be specified
//        if(conf.get(HbaseInputOptionsConf.INPUT_TABLE)==null){
//            throw new Exception("Input table name must be specified");
//        }
        conf.setBoolean(HbaseInputOptionsConf.MAPREDUCE_INPUT_AUTOBALANCE, sourceOptions.getBoolean(HbaseInputOptionsConf.MAPREDUCE_INPUT_AUTOBALANCE, true));
        conf.setInt(HbaseInputOptionsConf.NUM_MAPPERS_PER_REGION, sourceOptions.getInteger(HbaseInputOptionsConf.NUM_MAPPERS_PER_REGION, 1));

        conf.setBoolean(HbaseInputOptionsConf.SCAN_CACHEBLOCKS, sourceOptions.getBoolean(HbaseInputOptionsConf.SCAN_CACHEBLOCKS, false));
        conf.set(HbaseInputOptionsConf.SCAN_COLUMN_FAMILY, sourceOptions.getString(HbaseInputOptionsConf.SCAN_COLUMN_FAMILY, null));
        conf.setInt(HbaseInputOptionsConf.SCAN_CACHEDROWS, sourceOptions.getInteger(HbaseInputOptionsConf.INPUT_TABLE, 500));

//        conf.set(HbaseInputOptionsConf.MAX_AVERAGE_REGION_SIZE, sourceOptions.getString(HbaseInputOptionsConf.MAX_AVERAGE_REGION_SIZE, null));
        //if starStop key not specified, the start key and the stop key of the table will be collected.
        List<String> startStopKeys = manager.getTableStartStopKeys(conf.get(HbaseInputOptionsConf.INPUT_TABLE));
        conf.set(HbaseInputOptionsConf.SCAN_ROW_START, sourceOptions.getString(HbaseInputOptionsConf.SCAN_ROW_START, startStopKeys.get(0)));
        conf.set(HbaseInputOptionsConf.SCAN_ROW_STOP, sourceOptions.getString(HbaseInputOptionsConf.SCAN_ROW_STOP, startStopKeys.get(1)));

        // set timestamp for Scan
        conf.set(HbaseInputOptionsConf.SCAN_TIMERANGE_START,
                sourceOptions.getString(HbaseInputOptionsConf.SCAN_TIMERANGE_START, null));
        conf.set(HbaseInputOptionsConf.SCAN_TIMERANGE_END, sourceOptions.getString(HbaseInputOptionsConf.SCAN_TIMERANGE_END, null));
        conf.set(HbaseInputOptionsConf.SCAN_TIMESTAMP, sourceOptions.getString(HbaseInputOptionsConf.SCAN_TIMESTAMP, null));

        super.setConf(conf);
    }

    @Override
    public RecordReader createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException {

        RecordReader<ImmutableBytesWritable, Result> result =
                super.createRecordReader(split, context);

        HBaseRecordReader recordReader = new HBaseRecordReader(result);

        return recordReader;
    }
}

class HbaseRecordReader extends RecordReader<NullWritable, HerculesWritable> {

    private RecordReader<ImmutableBytesWritable, Result> result;
    protected List<WrapperGetter<Result>> wrapperGetterList;

    public HBaseRecordReader(RecordReader<ImmutableBytesWritable, Result> result) {
        this.result = result;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        result.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return result.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getCurrentValue().getMap();

        HerculesWritable res = new HerculesWritable();
        // TODO 根据框架，获得 HerculesWritable 并返回。WrapperGetterList -> HerculesWritable

        for (byte[] family : map.keySet()) {

            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for (byte[] column : familyMap.keySet()) {                              //根据列名循坏
                System.out.println(new String(family) + "：" + new String(column));
                NavigableMap<Long, byte[]> valuesMap = familyMap.get(column);

                for (Map.Entry<Long, byte[]> s : valuesMap.entrySet()) {                //获取列对应的不同版本数据，默认最新的一个

                    System.out.println(s.getKey() + "   " + new String(s.getValue(), "utf-8"));
                }
            }
        }
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return result.getProgress();
    }

    @Override
    public void close() throws IOException {
        result.close();
    }

}
