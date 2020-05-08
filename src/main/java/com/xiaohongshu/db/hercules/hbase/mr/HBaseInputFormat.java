package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.*;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataType;
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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import javax.sql.rowset.serial.SerialException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * 用 proxy 的方式，使用 TableInputFormat 来访问上游数据库，实现 split 和 recordReader 功能。
 */
public class HBaseInputFormat extends HerculesInputFormat<HBaseDataTypeConverter> implements HBaseManagerInitializer {

    private static final Log LOG = LogFactory.getLog(HBaseInputFormat.class);
    private HBaseManager manager;
    private GenericOptions sourceOptions;
    private Map<String, HBaseDataType> hbaseColumnTypeMap;

    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        hbaseColumnTypeMap = HBaseDataTypeConverter.convert(sourceOptions.getJson(HBaseOptionsConf.HBASE_COLUMN_TYPE_MAP, null));
        super.initializeContext(sourceOptions);
        this.sourceOptions = sourceOptions;
        manager = initializeManager(sourceOptions);
    }

    /**
     * split策略：默认一个region对应一个split，指定 num—mapper 后，可以合并region生成新的split
     */
    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        List<RegionInfo> rsInfo = manager.getRegionInfo(sourceOptions.getString(HBaseOptionsConf.TABLE, null));
        for(RegionInfo r: rsInfo){
            String startKey = Bytes.toString(r.getStartKey());
            String endKey = Bytes.toString(r.getEndKey());
            splits.add(new HBaseSplit(startKey,endKey));
        }
        splits.sort((o1, o2) -> {
            HBaseSplit s1 = (HBaseSplit) o1;
            HBaseSplit s2 = (HBaseSplit) o2;
            return s1.getStartKey().compareTo(s2.getStartKey());
        });
        int regionNum = splits.size();
        List<InputSplit> newSplits = new ArrayList<>();
        if(numSplits<splits.size()){
            int i = 0;
            int regionsPerSplit = (int) Math.ceil((float)regionNum/(float)numSplits);
            while(i<regionNum){
                if(newSplits.size()+regionNum-i+1==numSplits){
                    newSplits.addAll(splits.subList(i,splits.size()));
                    break;
                }
                HBaseSplit startRegion = (HBaseSplit) splits.get(i);
                String startKey = startRegion.getStartKey();
                i = i+regionsPerSplit-1;
                HBaseSplit endRegion;
                if(i<regionNum){
                    endRegion = (HBaseSplit) splits.get(i);
                }else{
                    endRegion = (HBaseSplit) splits.get(splits.size()-1);
                }
                String endKey = endRegion.getEndKey();
                newSplits.add(new HBaseSplit(startKey, endKey));
                i++;
            }
            splits = newSplits;
        }
        LOG.info(String.format("Actually split to %d splits: %s", splits.size(), splits.toString()));
        return splits;
    }

    @Override
    protected HerculesRecordReader innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new HBaseRecordReader(manager, converter, sourceOptions.getString(HBaseInputOptionsConf.ROW_KEY_COL_NAME, null), hbaseColumnTypeMap);
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

    @Override
    public String toString() {
        return '{'+startKey +" <=rowkey< " + endKey + '}';
    }
}

class HBaseRecordReader extends HerculesRecordReader<NavigableMap<Long, byte[]>, DataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(HBaseRecordReader.class);
    private HBaseManager manager;
    private String rowKeyCol = null;
    private ResultScanner scanner;
    private Result value;
    private Table table;
    private Map<String, HBaseDataType> hbaseColumnTypeMap;

    // 测试io等待时间
//    private Long ioTimeStatistics = new Long(0l);

    /**
     * @param manager
     * @param converter
     * @param rowKeyCol 用来作为rowKey的一列数据
     */
    public HBaseRecordReader(HBaseManager manager,  DataTypeConverter converter, String rowKeyCol, Map<String, HBaseDataType> hbaseColumnTypeMap) {

        super(converter);
        this.manager = manager;
        this.hbaseColumnTypeMap = hbaseColumnTypeMap;
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
    }

    @Override
    protected WrapperGetter getIntegerGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (columnValueMap, name, seq) -> {
            HBaseDataType dataType = hbaseColumnTypeMap.get(name);
            byte[] res = columnValueMap.firstEntry().getValue();
            if(null==res){
                return new NullWrapper();
            }
            switch(dataType){
                case SHORT:
                    return new IntegerWrapper(Bytes.toShort(res));
                case INT:
                    return new IntegerWrapper(Bytes.toInt(res));
                case LONG:
                    return new IntegerWrapper(Bytes.toLong(res));
                default:
                    throw new MapReduceException("Unknown data type: " + dataType.name());
            }
        };
    }

    @Override
    protected WrapperGetter getDoubleGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (columnValueMap, name, seq) -> {
            HBaseDataType dataType = hbaseColumnTypeMap.get(name);
            byte[] res = columnValueMap.firstEntry().getValue();
            if(null==res){
                return new NullWrapper();
            }
            switch(dataType){
                case FLOAT:
                    return new DoubleWrapper(Bytes.toFloat(res));
                case DOUBLE:
                    return new DoubleWrapper(Bytes.toDouble(res));
                case BIGDECIMAL:
                    return new DoubleWrapper(Bytes.toBigDecimal(res));
                default:
                    throw new MapReduceException("Unknown data type: " + dataType.name());
            }
        };
    }

    @Override
    protected WrapperGetter getBooleanGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (columnValueMap, name, seq) -> {
            byte[] res = columnValueMap.firstEntry().getValue();
            if (res==null) {
                return new NullWrapper();
            } else {
                return new BooleanWrapper(Bytes.toBoolean(res));
            }
        };
    }

    @Override
    protected WrapperGetter getStringGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (columnValueMap, name, seq) -> {
            byte[] res = columnValueMap.firstEntry().getValue();
            if (res==null) {
                return new NullWrapper();
            } else {
                return new StringWrapper(Bytes.toString(res));
            }
        };
    }

    // TODO 检查目前的转换能否正常work，借鉴自 dataX
    @Override
    protected WrapperGetter getDateGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (columnValueMap, name, seq) -> {
            byte[] res = columnValueMap.firstEntry().getValue();
            if (res==null) {
                return new NullWrapper();
            } else {
                String dateValue = Bytes.toStringBinary(res);
                // 需要设定一个dateformat，即 new String()
                return new DateWrapper(DateUtils.parseDate(dateValue, new String()));
            }
        };
    }

    @Override
    protected WrapperGetter getBytesGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (columnValueMap, name, seq) -> {
            byte[] res = columnValueMap.firstEntry().getValue();
            if (res==null) {
                return new NullWrapper();
            } else {
                return new BytesWrapper(res);
            }
        };
    }

    @Override
    protected WrapperGetter getNullGetter() {
        return (WrapperGetter<NavigableMap<Long, byte[]>>) (row, name, seq) -> NullWrapper.INSTANCE;
    }

    /**
     * 调用 tableRecordReader.nextKeyValue()，准备好新的一行数据（Result）
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
//        long startTime = System.currentTimeMillis();
        value = scanner.next();
//        ioTimeStatistics += (System.currentTimeMillis()-startTime);
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
        // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游
        if(rowKeyCol!=null){
            record.put(rowKeyCol,  new BytesWrapper(value.getRow()));
        }
        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for(int i=0;i<columnNum;i++){
                String columnName = columnNameList.get(i);
                NavigableMap<Long, byte[]> columnValueMap = familyMap.get(columnName.getBytes());
                if(columnValueMap==null){ // 上游没有该行数据，做忽略处理，并且做好log
                    LOG.warn("The Column "+columnName+" has not content in the record, skipping.");
                    continue;
                }
                for (Map.Entry<Long, byte[]> s : columnValueMap.entrySet()) {                //获取列对应的不同版本数据，默认最新的一个
                    record.put(columnName, getWrapperGetter(columnTypeMap.get(columnName)).get(columnValueMap, columnName, 0));
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

//        LOG.info("TIMESPENTFORIO: "+ioTimeStatistics.toString());
        scanner.close();
        table.close();
        manager.closeConnection();
    }
}