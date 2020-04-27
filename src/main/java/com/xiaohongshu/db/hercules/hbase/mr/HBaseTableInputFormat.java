package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
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
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
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

    /**
     * An important step to setup database connection（conf and scan）, make sure each conf option is correctly defined
     * @param conf
     */
    @SneakyThrows
    @Override
    public void setConf(Configuration conf) {

        HBaseManager.setSourceConf(conf, sourceOptions, manager);
        // 先调用 createScanFromConfiguration 创建 Scan，可以灵活增加设定（比如 Filter 等）
        Scan scan = TableInputFormat.createScanFromConfiguration(conf);
        List<String> columnNameList = Arrays.asList(sourceOptions.getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        // 根据用户给定的 ColumnNameList 对 scan 进行设置（白名单）。
        for(String col: columnNameList){
            scan.addColumn(conf.get(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY).getBytes(), col.getBytes());
        }
        conf.set(HBaseInputOptionsConf.SCAN, TableMapReduceUtil.convertScanToString(scan));
        super.setConf(conf);
    }

    @Override
    public HerculesRecordReader createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException {

        RecordReader<ImmutableBytesWritable, Result> tableRecordReader =
                super.createRecordReader(split, context);

        HBaseRecordReader recordReader = new HBaseRecordReader(tableRecordReader, converter, sourceOptions.getString(HBaseInputOptionsConf.ROW_KEY_COL_NAME, null));

        return recordReader;
    }
}

class HBaseRecordReader extends HerculesRecordReader<NavigableMap<Long, byte[]>, DataTypeConverter> {

    private RecordReader<ImmutableBytesWritable, Result> tableRecordReader;
    private String rowKeyCol = null;

    /**
     * HBaseRecordReader will reuse tableRecordReader to get a Result from Scanner and convert it to HerculesWritable
     * @param tableRecordReader {@link #RecordReader} created by TableInputFormat
     * @param converter
     * @param rowKeyCol
     */
    public HBaseRecordReader(RecordReader tableRecordReader,  DataTypeConverter converter, String rowKeyCol) {

        super(converter);
        this.tableRecordReader = tableRecordReader;
        if(rowKeyCol!=null){
            this.rowKeyCol = rowKeyCol;
        }
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        tableRecordReader.initialize(split, context);
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

    // TODO 检查目前的转换能否正常work
    @Override
    protected WrapperGetter getDateGetter() {
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> columnValueMap, String name, int seq) throws Exception {
                byte[] res = columnValueMap.firstEntry().getValue();
                if (res==null) {
                    return new NullWrapper();
                } else {
                    String dateValue = Bytes.toStringBinary(res);
                    // 需要设定一个dateformat，即 new String()
                    return new DateWrapper(DateUtils.parseDate(dateValue, new String()));
                }
            }
        };
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
        return tableRecordReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @SneakyThrows
    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = tableRecordReader.getCurrentValue().getMap();

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
        tableRecordReader.close();
    }
}