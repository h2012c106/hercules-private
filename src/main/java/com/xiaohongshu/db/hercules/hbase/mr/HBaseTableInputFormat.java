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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

// 所有的配置都传入 TableInputFormat， 和数据库之间的链接由 TableInputFormat 维护。
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
     * setup好conf和scan，以备TableInputFormat使用
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

    /**
     * 创建父类的 TableRecordReader,用于构造 HBaseRecordReader
     * @param split
     * @param context
     * @return
     * @throws IOException
     */
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
     * HBaseRecordReader 会基于 tableRecordReader 来获得 Result 并转换成 HerculesWritable，传到下游
     * @param tableRecordReader {@link #RecordReader} 由 TableInputFormat 创建
     * @param converter
     * @param rowKeyCol 用来作为rowKey的一列数据
     */
    public HBaseRecordReader(RecordReader tableRecordReader,  DataTypeConverter converter, String rowKeyCol) {

        super(converter);
        this.tableRecordReader = tableRecordReader;
        if(rowKeyCol!=null){
            this.rowKeyCol = rowKeyCol;
        }
    }

    /**
     * 传入 split 和 context 的时候直接传入 tableRecordReader，利用 tableRecordReader 来完成读数据。
     */
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

    // TODO 检查目前的转换能否正常work，借鉴自 dataX
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
        return new WrapperGetter<NavigableMap<Long, byte[]>>() {
            @Override
            public BaseWrapper get(NavigableMap<Long, byte[]> row, String name, int seq) throws Exception {
                return NullWrapper.INSTANCE;
            }
        };
    }

    /**
     * 调用 tableRecordReader.nextKeyValue()，准备好新的一行数据（Result）
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return tableRecordReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }


    /**
     * 从 tableRecordReader 中获得 NavigableMap， 遍历 map，将所有的数据放入 HerculesWritable 并返回。
     * @return HerculesWritable
     */
    @SneakyThrows
    @Override
    public HerculesWritable getCurrentValue(){

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = tableRecordReader.getCurrentValue().getMap();

        int columnNum = columnNameList.size();
        HerculesWritable record = new HerculesWritable(columnNum);

        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for(int i=0;i<columnNum;i++){
                String columnName = (String) columnNameList.get(i);
                // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游
                if(rowKeyCol!=null){
                    record.put(rowKeyCol, (BaseWrapper) getWrapperGetter(DataType.STRING));
                }
                NavigableMap<Long, byte[]> columnValueMap = familyMap.get(columnName.getBytes());

                for (Map.Entry<Long, byte[]> s : columnValueMap.entrySet()) {                //获取列对应的不同版本数据，默认最新的一个
                    record.put(columnName, wrapperGetterList.get(i).get(columnValueMap, columnName, 0));
                }
            }
        }
        return record;
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