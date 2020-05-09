package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
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
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;


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
     * 配置基本的链接数据库的参数，后续通过{@link #genScan}来进行详细的配置Scan
     */
    public void setBasicConf(){

        conf.set(HBaseOptionsConf.HB_ZK_QUORUM, options.getString(HBaseOptionsConf.HB_ZK_QUORUM,null));
        conf.set(HBaseOptionsConf.HB_ZK_PORT, options.getString(HBaseOptionsConf.HB_ZK_PORT,"2181"));
    }

    public Configuration getConf(){

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

        Admin admin = getConnection().getAdmin();
        List<RegionInfo> rsInfo = admin.getRegions(TableName.valueOf(tn));
        admin.close();
        return rsInfo;
    }

    public void closeConnection() throws IOException {
        if(conn!=null&& !conn.isClosed()){
            conn.close();
        }
    }

    /**
     * 返回一个新创建的Table连接
     * @return Table
     * @throws IOException
     */
    public Table getHtable() throws IOException {
        Connection conn = getConnection();
        System.out.println(options.getString(HBaseOptionsConf.TABLE,null));
        return conn.getTable(TableName.valueOf(options.getString(HBaseOptionsConf.TABLE,null)));
    }

    public Scan genScan(Scan scan, String startKey, String endKey) throws IOException {

        scan.withStartRow(Bytes.toBytes(startKey))
                .withStopRow(Bytes.toBytes(endKey));
        if(null!=options.getString(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY,null)){
            scan.addFamily(Bytes.toBytes(options.getString(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY,null)));
        }
        if((null!=options.getString(HBaseInputOptionsConf.SCAN_TIMERANGE_START, null))
                &&(null!=options.getLong(HBaseInputOptionsConf.SCAN_TIMERANGE_END, null))){
            scan.setTimeRange(options.getLong(HBaseInputOptionsConf.SCAN_TIMERANGE_START, null),
                    options.getLong(HBaseInputOptionsConf.SCAN_TIMERANGE_END, null));
        }
        if(null!=options.getInteger(HBaseInputOptionsConf.SCAN_CACHEDROWS, null)){
            scan.setCaching(options.getInteger(HBaseInputOptionsConf.SCAN_CACHEDROWS, null));
        }
        if(null!=options.getInteger(HBaseInputOptionsConf.SCAN_BATCHSIZE, null)){
            scan.setBatch(options.getInteger(HBaseInputOptionsConf.SCAN_BATCHSIZE, null));
        }
        // cache blocks 配置项不开放给用户
        scan.setCacheBlocks(false);
        return scan;
    }

    public static void setTargetConf(Configuration conf, GenericOptions targetOptions){


        HBaseManager.setConfParam(conf, HBaseOutputOptionsConf.COLUMN_FAMILY, targetOptions, true);
        HBaseManager.setConfParam(conf, HBaseOptionsConf.TABLE, targetOptions, true);
        HBaseManager.setConfParam(conf, HBaseOutputOptionsConf.ROW_KEY_COL_NAME, targetOptions, true);
        conf.setInt(HBaseOutputOptionsConf.MAX_WRITE_THREAD_NUM,
                targetOptions.getInteger(HBaseOutputOptionsConf.MAX_WRITE_THREAD_NUM, HBaseOutputOptionsConf.DEFAULT_MAX_WRITE_THREAD_NUM));
        conf.setLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE,
                targetOptions.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, HBaseOutputOptionsConf.DEFAULT_WRITE_BUFFER_SIZE));
    }

    public static void setConfParam(Configuration conf, String paramName, GenericOptions options, boolean notNull){
        conf.set(paramName, options.getString(paramName, null));
        if(notNull&&(conf.get(paramName)==null)){
            throw new ParseException("The param should not be null: "+paramName);
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
        String userTable = conf.get(HBaseOptionsConf.TABLE);
        long writeBufferSize = conf.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, HBaseOutputOptionsConf.DEFAULT_WRITE_BUFFER_SIZE);
        Connection hConnection = manager.getConnection();
        TableName hTableName = TableName.valueOf(userTable);
        Admin admin = null;
        BufferedMutator bufferedMutator;
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
