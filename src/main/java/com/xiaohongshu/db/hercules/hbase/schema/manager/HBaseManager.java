package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseManager {

    private static final Log LOG = LogFactory.getLog(HBaseManager.class);

    private GenericOptions options;
    private Connection conn = null;
    private Configuration conf = null;

    public HBaseManager(GenericOptions options) {
        this.options = options;
    }

    public Connection getConnection() throws IOException {
        // return connection if connection already established and not closed
        if(conn!=null && !conn.isClosed()){
            return conn;
        }
        conn = ConnectionFactory.createConnection(getConf());
        return conn;
    }

    public void setSourceConf(){
        // set connection
        conf.set(HBaseOptionsConf.HB_ZK_QUORUM, options.getString(HBaseOptionsConf.HB_ZK_QUORUM,null));
        conf.set(HBaseOptionsConf.HB_ZK_PORT, options.getString(HBaseOptionsConf.HB_ZK_PORT,"2182"));
    }

    public Configuration getConf(){
        // return conf if conf already set
        if(conf != null){
            return conf;
        }
        conf = new Configuration();
        setSourceConf();
        return conf;
    }

    public List<RegionInfo> getRegionInfo(String tn) throws IOException {
        Admin admin = conn.getAdmin();
        List<RegionInfo> rsInfo = admin.getRegions(TableName.valueOf(tn));
        admin.close();
        return rsInfo;
    }

    public List<String> getTableStartStopKeys(String tn) throws IOException {
        List<RegionInfo> rsInfo = getRegionInfo(tn);
        List<String> startStopKeys = new ArrayList<>();
        startStopKeys.add(new String(rsInfo.get(0).getStartKey()));
        startStopKeys.add(new String(rsInfo.get(1).getEndKey()));
        return startStopKeys;
    }

    public void closeConnection() throws IOException {
        if(conn!=null&& !conn.isClosed()){
            conn.close();
        }
    }

    /**
     * 上下游的的 conf 的配置放到 HBaseManager 中来完成
     * @param conf
     * @param sourceOptions
     * @param manager
     * @throws IOException
     */
    public void setSourceConf(Configuration conf, GenericOptions sourceOptions, HBaseManager manager) throws IOException {

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
    }

    public void setTargetConf(Configuration conf, GenericOptions targetOptions, HBaseManager manager){

        conf.set(HBaseOutputOptionsConf.COLUMN_FAMILY,
                targetOptions.getString(HBaseOutputOptionsConf.COLUMN_FAMILY,null));

        conf.setInt(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM,
                targetOptions.getInteger(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM, HBaseOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM));

        conf.setInt(HBaseOutputOptionsConf.PUT_BATCH_SIZE,
                targetOptions.getInteger(HBaseOutputOptionsConf.PUT_BATCH_SIZE,HBaseOutputOptionsConf.DEFAULT_PUT_BATCH_SIZE));

        conf.set(HBaseOutputOptionsConf.OUTPU_TABLE, targetOptions.getString(HBaseOutputOptionsConf.OUTPU_TABLE,null));

        conf.set(HBaseOutputOptionsConf.ROW_KEY_COL_NAME, targetOptions.getString(HBaseOutputOptionsConf.ROW_KEY_COL_NAME,null));
    }
}
