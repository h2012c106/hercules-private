package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * 用 proxy 的方式，使用 TableInputFormat 来访问上游数据库，实现 split 和 recordReader 功能。
 */
public class HBaseInputFormat extends HerculesInputFormat<HBaseDataTypeConverter> implements HBaseManagerInitializer {

    private HBaseManager manager;
    private GenericOptions sourceOptions;

    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        super.initializeContext(sourceOptions);
        this.sourceOptions = sourceOptions;
        manager = initializeManager(sourceOptions);
    }

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        List<RegionInfo> rsInfo = manager.getRegionInfo(sourceOptions.getString(HBaseInputOptionsConf.TABLE, null));
        for(RegionInfo r: rsInfo){
            String startKey = Bytes.toString(r.getStartKey());
            String endKey = Bytes.toString(r.getEndKey());
            splits.add(new HBaseSplit(startKey,endKey));
        }
//        splits.remove(0);
//        splits.remove(splits.size()-1);
        return splits;
    }

    @Override
    protected HerculesRecordReader innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new HBaseRecordReader(manager, converter, sourceOptions.getString(HBaseInputOptionsConf.ROW_KEY_COL_NAME, null));
    }

    @Override
    public HBaseDataTypeConverter initializeConverter() {
        return new HBaseDataTypeConverter();
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}

class HBaseSplit extends InputSplit implements Writable {

    private String startKey;
    private String endKey;

    public HBaseSplit() {
    }

    public HBaseSplit(String startKey, String endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public String getStartKey(){
        return startKey;
    }
    
    public String getEndKey(){
        return endKey;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, this.startKey);
        Text.writeString(dataOutput, this.endKey);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.startKey = Text.readString(dataInput);
        this.endKey = Text.readString(dataInput);
    }
}

class HBaseRecordReader extends HerculesRecordReader<NavigableMap<Long, byte[]>, DataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(HBaseRecordReader.class);
    private HBaseManager manager;
    private String rowKeyCol = null;
    private ResultScanner scanner;
    private Result value;
    private Table table;

    /**
     * @param manager
     * @param converter
     * @param rowKeyCol 用来作为rowKey的一列数据
     */
    public HBaseRecordReader(HBaseManager manager,  DataTypeConverter converter, String rowKeyCol) {

        super(converter);
        this.manager = manager;
        if(rowKeyCol!=null){
            this.rowKeyCol = rowKeyCol;
        }
    }

    /**
     * 传入 split 和 context 的时候直接传入 tableRecordReader，利用 tableRecordReader 来完成读数据。
     */
    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        HBaseSplit hbaseSplit = (HBaseSplit) split;
        table = manager.getHtable();
        Scan scan = new Scan();
        scanner = table.getScanner(manager.genScan(scan, hbaseSplit.getStartKey(), hbaseSplit.getEndKey()));
        LOG.info("scan: startrow: "+scan.getStartRow()+"   endrow: "+scan.getStopRow());
        LOG.info("tableName:"+table.getName()+" startkey: "+hbaseSplit.getStartKey()+" endkey:  "+hbaseSplit.getEndKey()+" scanner: "+scanner.toString());
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
        value = scanner.next();
        if(null!=value){
            return true;
        }
        return false;
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

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = value.getMap();

        int columnNum = columnNameList.size();
        HerculesWritable record = new HerculesWritable(columnNum);
        if(rowKeyCol!=null){
            record.put(rowKeyCol,  new BytesWrapper(value.getRow()));
        }
        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for(int i=0;i<columnNum;i++){
                String columnName = (String) columnNameList.get(i);
                // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游
                NavigableMap<Long, byte[]> columnValueMap = familyMap.get(columnName.getBytes());

                for (Map.Entry<Long, byte[]> s : columnValueMap.entrySet()) {                //获取列对应的不同版本数据，默认最新的一个
                    record.put(columnName, wrapperGetterList.get(i).get(columnValueMap, columnName, 0));
                }
            }
        }
        LOG.info("FROM RECORD!: "+record.toString());

        return record;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        scanner.close();
        table.close();
    }
}