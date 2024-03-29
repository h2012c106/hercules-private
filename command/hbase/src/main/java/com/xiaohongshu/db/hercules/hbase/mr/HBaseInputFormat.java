package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BytesWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.core.utils.entity.StingyMap;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf.TABLE;

/**
 * 通过获取的regions list 来生成splits。最少一个region对应一个split
 */
public class HBaseInputFormat extends HerculesInputFormat<byte[]> {

    private static final Log LOG = LogFactory.getLog(HBaseInputFormat.class);

    @Assembly
    private HBaseManager manager;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @SchemaInfo
    private Schema schema;

    private Connection connection;

    /**
     * split策略：默认一个region对应一个split，指定 num—mapper 后，可以合并region生成新的split。
     */
    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        Connection connection = manager.getConnection(context.getConfiguration());
        List<RegionInfo> rsInfo = HBaseManager.getRegionInfo(connection, sourceOptions.getString(TABLE, null));
        for (RegionInfo r : rsInfo) {
            String startKey = Bytes.toString(r.getStartKey());
            String endKey = Bytes.toString(r.getEndKey());
            splits.add(new HBaseSplit(startKey, endKey));
        }
        splits.sort((o1, o2) -> {
            HBaseSplit s1 = (HBaseSplit) o1;
            HBaseSplit s2 = (HBaseSplit) o2;
            return s1.getStartKey().compareTo(s2.getStartKey());
        });
        filterSplits(splits, sourceOptions.getString(HBaseInputOptionsConf.SCAN_ROW_START, null),
                sourceOptions.getString(HBaseInputOptionsConf.SCAN_ROW_STOP, null));
        int regionNum = splits.size();
        List<InputSplit> newSplits = new ArrayList<>();
        if (numSplits < splits.size()) {
            if (numSplits == 1) {
                LOG.warn("Map set to 1, only use 1 map.");
            }
            int i = 0;
            int regionsPerSplit = (int) Math.ceil((float) regionNum / (float) numSplits);
            while (i < regionNum) {
                if (newSplits.size() + regionNum - i + 1 == numSplits) {
                    newSplits.addAll(splits.subList(i, splits.size()));
                    break;
                }
                HBaseSplit startRegion = (HBaseSplit) splits.get(i);
                String startKey = startRegion.getStartKey();
                i = i + regionsPerSplit - 1;
                HBaseSplit endRegion;
                if (i < regionNum) {
                    endRegion = (HBaseSplit) splits.get(i);
                } else {
                    endRegion = (HBaseSplit) splits.get(splits.size() - 1);
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
     *
     * @param rowStartKey （inclusive）
     * @param rowStopKey  （exclusive）
     */
    private void filterSplits(List<InputSplit> splits, String rowStartKey, String rowStopKey) {
        HBaseSplit theSplit;
        if (null != rowStartKey && null != rowStopKey) {
            if (rowStartKey.compareTo(rowStopKey) >= 0) {
                throw new ParseException("rowStopKey must be larger than rowStartKey. Please check input.");
            }
        }
        if (null != rowStartKey) {
            LOG.info("Row start key is set to: " + rowStartKey + " (inclusive)");
            theSplit = (HBaseSplit) splits.get(0);
            while (rowStartKey.compareTo(theSplit.getEndKey()) > 0) {
                splits.remove(0);
                theSplit = (HBaseSplit) splits.get(0);
            }
            if (theSplit.getEndKey().equals(rowStartKey)) {
                splits.remove(0);
            } else {
                theSplit.setStartKey(rowStartKey);
            }
        }
        if (null != rowStopKey) {
            LOG.info("Row stop key is set to: " + rowStopKey + " (exclusive)");
            theSplit = (HBaseSplit) splits.get(splits.size() - 1);
            while (rowStopKey.compareTo(theSplit.getStartKey()) < 0) {
                splits.remove(splits.size() - 1);
                theSplit = (HBaseSplit) splits.get(splits.size() - 1);
            }
            if (theSplit.getStartKey().equals(rowStopKey)) {
                splits.remove(splits.size() - 1);
            } else {
                theSplit.setEndKey(rowStopKey);
            }
        }
    }

    @Override
    protected HerculesRecordReader<byte[]> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) {
        String rowKeyCol = sourceOptions.getString(KEY_NAME, null);
        if (rowKeyCol != null) {
            LOG.info("rowKeyCol name has been set to: " + rowKeyCol);
        } else {
            LOG.info("rowKeyCol name not set. rowKey will be excluded.");
        }
        return new HBaseRecordReader(context, manager, rowKeyCol);
    }

    @Override
    protected WrapperGetterFactory<byte[]> createWrapperGetterFactory() {
        return new HBaseInputWrapperManager();
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

    public String getStartKey() {
        return startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public void setEndKey(String endKey) {
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
        return '{' + startKey + " <=rowkey< " + endKey + '}';
    }
}

class HBaseRecordReader extends HerculesRecordReader<byte[]> implements InjectedClass {

    private static final Log LOG = LogFactory.getLog(HBaseRecordReader.class);

    private final String rowKeyCol;
    private ResultScanner scanner;
    private Result value;
    private String columnFamily;

    private List<String> columnNameList;

    @Assembly
    private final HBaseManager manager = null;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    @SchemaInfo
    private Schema schema;

    @SneakyThrows
    public HBaseRecordReader(TaskAttemptContext context, HBaseManager manager, String rowKeyCol) {
        super(context);
        this.rowKeyCol = rowKeyCol;
    }

    @Override
    public void afterInject() {
        this.columnFamily = options.getString(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY, "");
        schema.setColumnTypeMap(new StingyMap<>(schema.getColumnTypeMap()));
    }

    /**
     * 创建scanner，通过scanner.next()不断获取新的数据。
     */
    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException {
        HBaseSplit hbaseSplit = (HBaseSplit) split;
        Connection connection = manager.getConnection(context.getConfiguration());
        Table table = HBaseManager.getHtable(connection, options.getString(TABLE, null));
        Scan scan = new Scan();
        scanner = table.getScanner(HBaseManager.genScan(scan, hbaseSplit.getStartKey(), hbaseSplit.getEndKey(), options));

        List<String> temp = new ArrayList<>(schema.getColumnNameList());
        temp.remove(rowKeyCol);
        columnNameList = temp;
        if (columnNameList.size() == 0 && !options.getBoolean(HBaseInputOptionsConf.IGNORE_COLUMN_SIZE_CHECK, false)) {
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
        HerculesWritable record;

        int columnNum = columnNameList.size();
        record = new HerculesWritable(columnNum);
        getLatestRecord(record, columnNum);

        // 如果用户指定了 row key col，则将 row key col 存入 HerculesWritable 并传到下游,否则则抛弃
        if (rowKeyCol != null) {
            record.put(rowKeyCol, BytesWrapper.get(value.getRow()));
        }
        return record;
    }

//    protected HerculesWritable getConvertedLatestRecord() throws IOException {
//        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyColMap = value.getNoVersionMap();
//        NavigableMap<byte[], byte[]> colValMap = familyColMap.get(columnFamily.getBytes());
//        String qualifier = options.getSourceOptions().getString(HBaseInputOptionsConf.KV_COLUMN, "");
//        byte[] val = colValMap.get(Bytes.toBytes(qualifier));
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("QUALIFIER: " + qualifier);
//        }
//        return kvSerializerSupplier.getKvSerializer().generateHerculesWritable(val, options.getSourceOptions());
//    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        value = scanner.next();
        return null != value;
    }

    /**
     * 获取 san 出来的 timestamp 范围中最新的一个版本
     */
    private void getLatestRecord(HerculesWritable record, int columnNum) throws Exception {
        // 利用getNoVersionMap可以获取包含最新版本的唯一数据。
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyColMap = value.getNoVersionMap();
        for (byte[] family : familyColMap.keySet()) {
            NavigableMap<byte[], byte[]> colValMap = familyColMap.get(family);
            // HBase 没有schema，也没有从下游获得schema
            // TODO 更好的逻辑？
            if (columnNum == 0) {
                for (Map.Entry<byte[], byte[]> entry : colValMap.entrySet()) {
                    record.put(new String(entry.getKey()), getWrapperGetter(BaseDataType.BYTES).get(entry.getValue(), null, null, 0));
                }
            } else {
                for (int i = 0; i < columnNum; i++) {
                    String qualifier = columnNameList.get(i);
                    byte[] val = colValMap.get(Bytes.toBytes(qualifier));
                    record.put(qualifier, getWrapperGetter(schema.getColumnTypeMap().get(qualifier)).get(val, null, null, 0));
                }
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
        if (rowKeyCol != null) {
            record.put(rowKeyCol, BytesWrapper.get(value.getRow()));
        }
        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据

            for (String qualifier : columnNameList) {
                NavigableMap<Long, byte[]> columnValueMap = familyMap.get(qualifier.getBytes());
                if (columnValueMap == null) { // 上游没有该行数据，做忽略处理，并且做好log
                    if (LOG.isDebugEnabled()) {
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
    public float getProgress() {
        return 0;
    }
}