package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * 通过获取的regions list 来生成splits。最少一个region对应一个split
 */
public class HBaseInputFormat extends HerculesInputFormat<HBaseDataTypeConverter> implements HBaseManagerInitializer {

    private static final Log LOG = LogFactory.getLog(HBaseInputFormat.class);
    private HBaseManager manager;
    private GenericOptions sourceOptions;

    @SneakyThrows
    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        super.initializeContext(sourceOptions);
        this.sourceOptions = sourceOptions;
        manager = initializeManager(sourceOptions);
    }

    /**
     * split策略：默认一个region对应一个split，指定 num—mapper 后，可以合并region生成新的split。
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
        filterSplits(splits, sourceOptions.getString(HBaseInputOptionsConf.SCAN_ROW_START, null),
                sourceOptions.getString(HBaseInputOptionsConf.SCAN_ROW_STOP,null));
        int regionNum = splits.size();
        List<InputSplit> newSplits = new ArrayList<>();
        if(numSplits<splits.size()){
            if(numSplits==1){
                LOG.warn("Map set to 1, only use 1 map.");
            }
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


    /**
     * 若用户指定scan的起始rowkey，则根据指定的rowkey对splits列表进行过滤和重新设置。
     * @param rowStartKey （inclusive）
     * @param rowStopKey （exclusive）
     */
    private void filterSplits(List<InputSplit> splits, String rowStartKey, String rowStopKey){
        HBaseSplit theSplit;
        if(null!=rowStartKey&&null!=rowStopKey){
            if(rowStartKey.compareTo(rowStopKey)>=0){
                throw new ParseException("rowStopKey must be larger than rowStartKey. Please check input.");
            }
        }
        if(null!=rowStartKey){
            LOG.info("Row start key is set to: "+rowStartKey+" (inclusive)");
            theSplit = (HBaseSplit) splits.get(0);
            while(rowStartKey.compareTo(theSplit.getEndKey())>0){
                splits.remove(0);
                theSplit = (HBaseSplit) splits.get(0);
            }
            if(theSplit.getEndKey().equals(rowStartKey)){
                splits.remove(0);
            }else{
                theSplit.setStartKey(rowStartKey);
            }
        }
        if(null!=rowStopKey){
            LOG.info("Row stop key is set to: "+rowStopKey+" (exclusive)");
            theSplit = (HBaseSplit) splits.get(splits.size()-1);
            while(rowStopKey.compareTo(theSplit.getStartKey())<0){
                splits.remove(splits.size()-1);
                theSplit = (HBaseSplit) splits.get(splits.size()-1);
            }
            if(theSplit.getStartKey().equals(rowStopKey)){
                splits.remove(splits.size()-1);
            }else{
                theSplit.setEndKey(rowStopKey);
            }
        }
    }

    @Override
    protected HerculesRecordReader innerCreateRecordReader(InputSplit split, TaskAttemptContext context) {
        String rowKeyCol = sourceOptions.getString(HBaseOptionsConf.ROW_KEY_COL_NAME, null);
        if(rowKeyCol!=null){
            LOG.info("rowKeyCol name has been set to: "+rowKeyCol);
        }else{
            LOG.info("rowKeyCol name not set. rowKey will be excluded.");
        }
        return new HBaseRecordReader(manager, converter, rowKeyCol);
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }

    @Override
    public HBaseDataTypeConverter generateConverter() {
        return new HBaseDataTypeConverter();
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

    public void setStartKey(String startKey){
        this.startKey = startKey;
    }

    public void setEndKey(String endKey){
        this.endKey = endKey;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String[] getLocations() {
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

class HBaseRecordReader extends HerculesRecordReader<byte[], DataTypeConverter> {

    private static final Log LOG = LogFactory.getLog(HBaseRecordReader.class);
    private final HBaseManager manager;
    private final String rowKeyCol;
    private ResultScanner scanner;
    private Result value;

    @SneakyThrows
    public HBaseRecordReader(HBaseManager manager,  DataTypeConverter converter, String rowKeyCol){

        super(converter, new HBaseInputWrapperManager());
        this.manager = manager;
        this.rowKeyCol = rowKeyCol;

        // 测试用
//        manager.InsertTestDataToHBaseTable();
    }

    /**
     * 创建scanner，通过scanner.next()不断获取新的数据。
     */
    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException {
        HBaseSplit hbaseSplit = (HBaseSplit) split;
        Table table = manager.getHtable();
        Scan scan = new Scan();
        scanner = table.getScanner(manager.genScan(scan, hbaseSplit.getStartKey(), hbaseSplit.getEndKey()));

        List<String> temp = new ArrayList<>(columnNameList);
        temp.remove(rowKeyCol);
        columnNameList = temp;
        if (columnNameList.size() == 0) {
            throw new RuntimeException("Column name list failed to fetch(no column name found).");
        }
    }

    @Override
    public void innerClose() throws IOException {
//        scanner.close();
//        table.close();
        manager.closeConnection();
    }

    /**
     * 从 tableRecordReader 中获得 NavigableMap， 遍历 map，将所有的数据放入 HerculesWritable 并返回。
     */
    @SneakyThrows
    @Override
    protected HerculesWritable innerGetCurrentValue() {

        int columnNum = columnNameList.size();
        HerculesWritable record = new HerculesWritable(columnNum);
        // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游,否则则抛弃
        if(rowKeyCol!=null){
            record.put(rowKeyCol,  new BytesWrapper(value.getRow()));
        }
        getLatestRecord(record, columnNum);
        return record;
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        value = scanner.next();
        return null != value;
    }

    /**
     * 获取 san 出来的 timestamp 范围中最新的一个版本
     */
    private void  getLatestRecord(HerculesWritable record, int columnNum) throws Exception {

        // 利用getNoVersionMap可以获取包含最新版本的唯一数据。
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyColMap = value.getNoVersionMap();
        for (byte[] family : familyColMap.keySet()) {
            NavigableMap<byte[], byte[]> colValMap = familyColMap.get(family);
            for(int i=0;i<columnNum;i++){
                String qualifier = columnNameList.get(i);
                byte[] val = colValMap.get(Bytes.toBytes(qualifier));
                if(null==val){
                    if(LOG.isDebugEnabled()){
                        LOG.debug("The Column "+qualifier+" has no content in the record, skipping.");
                    }
                    continue;
                }
                if(LOG.isDebugEnabled()){
                    LOG.debug("QUALIFIER: "+qualifier);
                }
                record.put(qualifier, getWrapperGetter(columnTypeMap.get(qualifier)).get(val, qualifier, "", 0));
            }
        }
    }

    /**
     * 注意！暂时没有 implement multiVersion 的功能，如果需要，则要解决 一个 rowkey 映射多条数据的问题，并且把 timestamp 正确传给下游
     * 可用 recordBuffer实现（先查 recordBuffer，再获取下一个数据）
     */
    private HerculesWritable getMultiVersionRecord() throws Exception {

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = value.getMap();

        int columnNum = columnNameList.size();
        HerculesWritable record = new HerculesWritable(columnNum);
        // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游,否则则抛弃
        if(rowKeyCol!=null){
            record.put(rowKeyCol,  new BytesWrapper(value.getRow()));
        }
        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for (String qualifier : columnNameList) {
                NavigableMap<Long, byte[]> columnValueMap = familyMap.get(qualifier.getBytes());
                if (columnValueMap == null) { // 上游没有该行数据，做忽略处理，并且做好log
                    if(LOG.isDebugEnabled()){
                        LOG.warn("The Column " + qualifier + " has no content in the record, skipping.");
                    }
                    continue;
                }
                // TODO 如何用正确的姿势将两个版本的数据传递到下游，目前放一个就break出去
                for (Map.Entry<Long, byte[]> entry : columnValueMap.entrySet()) {
//                    record.put(qualifier, getWrapperGetter(columnTypeMap.get(qualifier)).get(entry, qualifier, 0,0));
                    break;
                }
            }
        }
        return record;
    }

    @Override
    public float getProgress(){
        return 0;
    }
}