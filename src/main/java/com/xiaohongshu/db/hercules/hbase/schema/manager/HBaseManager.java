package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import lombok.SneakyThrows;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

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

    /**
     * 配置最基本的链接数据库的参数，后续还需要通过{@link #setSourceConf}和{@link #setTargetConf}来分别进行详细的配置
     */
    public void setBasicConf(){
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
        setBasicConf();
        return conf;
    }

    /**
     * 通过表名和创建好的Connection来获取regionList
     * @param tn 表名
     * @return
     * @throws IOException
     */
    public List<RegionInfo> getRegionInfo(String tn) throws IOException {
        Admin admin = conn.getAdmin();
        List<RegionInfo> rsInfo = admin.getRegions(TableName.valueOf(tn));
        admin.close();
        return rsInfo;
    }

    /**
     * 通过获得的regionList，返回整个表的startKey和StopKey。默认是整个表全量导出。
     * @param tn
     * @return
     * @throws IOException
     */
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
    public static void setSourceConf(Configuration conf, GenericOptions sourceOptions, HBaseManager manager) throws IOException {

        conf.set(HBaseInputOptionsConf.INPUT_TABLE, sourceOptions.getString(HBaseInputOptionsConf.INPUT_TABLE, null));
        // input table name must be specified
//        if(conf.get(HbaseInputOptionsConf.INPUT_TABLE)==null){
//            throw new Exception("Input table name must be specified");
//        }
        conf.setBoolean(HBaseInputOptionsConf.MAPREDUCE_INPUT_AUTOBALANCE, sourceOptions.getBoolean(HBaseInputOptionsConf.MAPREDUCE_INPUT_AUTOBALANCE, true));
        conf.setInt(HBaseInputOptionsConf.NUM_MAPPERS_PER_REGION, sourceOptions.getInteger(HBaseInputOptionsConf.NUM_MAPPERS_PER_REGION, 1));

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

    public static void setTargetConf(Configuration conf, GenericOptions targetOptions){

        conf.set(HBaseOutputOptionsConf.COLUMN_FAMILY,
                targetOptions.getString(HBaseOutputOptionsConf.COLUMN_FAMILY,null));

        conf.setInt(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM,
                targetOptions.getInteger(HBaseOutputOptionsConf.EXECUTE_THREAD_NUM, HBaseOutputOptionsConf.DEFAULT_EXECUTE_THREAD_NUM));

        conf.set(HBaseOutputOptionsConf.OUTPU_TABLE, targetOptions.getString(HBaseOutputOptionsConf.OUTPU_TABLE,null));
        conf.set(HBaseOutputOptionsConf.ROW_KEY_COL_NAME, targetOptions.getString(HBaseOutputOptionsConf.ROW_KEY_COL_NAME,null));

        if(targetOptions.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE,null)!=null){
            conf.setLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, targetOptions.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE,null));
        }

    }


    /**
     * 通过配置好的conf以及manager来获取BufferedMutator.
     * @param conf
     * @param manager
     * @return
     * @throws IOException
     */
    public static BufferedMutator getBufferedMutator(Configuration conf, HBaseManager manager) throws IOException {
        String userTable = conf.get(HBaseOutputOptionsConf.OUTPU_TABLE);
        long writeBufferSize = conf.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, HBaseOutputOptionsConf.DEFAULT_WRITE_BUFFER_SIZE);
        Connection hConnection = manager.getConnection();
        TableName hTableName = TableName.valueOf(userTable);
        Admin admin = null;
        BufferedMutator bufferedMutator = null;
        try {
            admin = hConnection.getAdmin();
            bufferedMutator = hConnection.getBufferedMutator(
                    new BufferedMutatorParams(hTableName)
                            .pool(HTable.getDefaultExecutor(conf))
                            .writeBufferSize(writeBufferSize));
        } catch (Exception e) {
            closeAdmin(admin);
            closeConnection(hConnection);
            throw new RuntimeException("Failed to create BufferedMutator");
        }
        return bufferedMutator;
    }

    @SneakyThrows
    public static void closeAdmin(Admin admin){
        if(null!=admin){
            admin.close();
        }
    }

    @SneakyThrows
    public static void closeConnection(Connection conn){
        if(null!=conn){
            conn.close();
        }
    }
}
