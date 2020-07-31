package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase.option.HBaseInputOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseManager {

    private static final Log LOG = LogFactory.getLog(HBaseManager.class);

    private final GenericOptions options;
    private volatile static Connection conn;
    private Configuration conf = null;

    public HBaseManager(GenericOptions options) {
        this.options = options;
    }

    public Connection getConnection() throws IOException {
        if (null == conn) {
            synchronized (Connection.class) {
                if (null == conn) {
                    conn = ConnectionFactory.createConnection(getConf());
                }
            }
        }
        return conn;
    }

    /**
     * 配置基本的链接数据库的参数，后续通过{@link #genScan}来进行详细的配置Scan
     */
    public void setBasicConf() {

        conf.set("hbase.zookeeper.quorum", options.getString(HBaseOptionsConf.HB_ZK_QUORUM, null));
        conf.set("hbase.zookeeper.property.clientPort", options.getString(HBaseOptionsConf.HB_ZK_PORT, "2181"));
        conf.set("hbase.client.retries.number", "3");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    public Configuration getConf() {

        if (conf != null) {
            return conf;
        }
        conf = new Configuration();
        setBasicConf();
        return conf;
    }

    public List<RegionInfo> getRegionInfo(String tn) throws IOException {

        Admin admin = getConnection().getAdmin();
        List<RegionInfo> rsInfo = admin.getRegions(TableName.valueOf(tn));
        admin.close();
        return rsInfo;
    }

    public void closeConnection() throws IOException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public Table getHtable() throws IOException {
        Connection conn = getConnection();
        System.out.println(options.getString(HBaseOptionsConf.TABLE, null));
        return conn.getTable(TableName.valueOf(options.getString(HBaseOptionsConf.TABLE, null)));
    }

    public Scan genScan(Scan scan, String startKey, String endKey) throws IOException {

        scan.withStartRow(Bytes.toBytes(startKey))
                .withStopRow(Bytes.toBytes(endKey));
        String scanColumnFamily = options.getString(HBaseInputOptionsConf.SCAN_COLUMN_FAMILY, null);
        if (null == scanColumnFamily) {
            throw new ParseException("Scan column family can't be null.");
        }
        scan.addFamily(Bytes.toBytes(scanColumnFamily));
        if ((null != options.getString(HBaseInputOptionsConf.SCAN_TIMERANGE_START, null))
                && (null != options.getLong(HBaseInputOptionsConf.SCAN_TIMERANGE_END, null))) {
            scan.setTimeRange(options.getLong(HBaseInputOptionsConf.SCAN_TIMERANGE_START, null),
                    options.getLong(HBaseInputOptionsConf.SCAN_TIMERANGE_END, null));
        }
        if (null != options.getInteger(HBaseInputOptionsConf.SCAN_CACHEDROWS, null)) {
            scan.setCaching(options.getInteger(HBaseInputOptionsConf.SCAN_CACHEDROWS, null));
        }
        if (null != options.getInteger(HBaseInputOptionsConf.SCAN_BATCHSIZE, null)) {
            scan.setBatch(options.getInteger(HBaseInputOptionsConf.SCAN_BATCHSIZE, null));
        }
        List<String> scanColumns = Arrays.asList(options.getStringArray(BaseDataSourceOptionsConf.COLUMN, new String[]{}));
        if (scanColumns.size() != 0) {
            for (String qualifier : scanColumns) {
                scan.addColumn(Bytes.toBytes(scanColumnFamily), Bytes.toBytes(qualifier));
            }
        }
        // cache blocks 配置项不开放给用户
        scan.setCacheBlocks(false);
        return scan;
    }

    public static void setTargetConf(Configuration conf, GenericOptions targetOptions) {

        HBaseManager.setConfParam(conf, HBaseOutputOptionsConf.COLUMN_FAMILY, targetOptions, true);
        HBaseManager.setConfParam(conf, HBaseOptionsConf.TABLE, targetOptions, true);
        HBaseManager.setConfParam(conf, HBaseOptionsConf.ROW_KEY_COL_NAME, targetOptions, true);
        conf.setInt("hbase.htable.threads.max",
                targetOptions.getInteger(HBaseOutputOptionsConf.MAX_WRITE_THREAD_NUM, HBaseOutputOptionsConf.DEFAULT_MAX_WRITE_THREAD_NUM));
        conf.setLong("hbase.mapreduce.writebuffersize",
                targetOptions.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, HBaseOutputOptionsConf.DEFAULT_WRITE_BUFFER_SIZE));
    }

    public static void setConfParam(Configuration conf, String paramName, GenericOptions options, boolean notNull) {
        conf.set(paramName, options.getString(paramName, null));
        if (notNull && (conf.get(paramName) == null)) {
            throw new ParseException("The param should not be null: " + paramName);
        }
    }

    public static Table getTable(Configuration conf, HBaseManager manager) throws IOException {
        String userTable = conf.get(HBaseOptionsConf.TABLE);
        TableName hTableName = TableName.valueOf(userTable);
        if (!tableExists(hTableName, manager)){
            throw new RuntimeException("Table "+ hTableName.getNameAsString() +" not exists in HBase. Please make sure the table is created before running this task.");
        }
        return manager.getConnection().getTable(hTableName);
    }

    public static boolean tableExists(TableName tableName, HBaseManager manager) throws IOException {
        Admin hbaseAdmin = manager.getConnection().getAdmin();
        return hbaseAdmin.tableExists(tableName);
    }

    /**
     * 通过配置好的conf以及manager来获取BufferedMutator(Async).
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

    public static void closeAdmin(Admin admin) throws IOException {
        if (null != admin) {
            admin.close();
        }
    }

    public static void closeConnection(Connection conn) throws IOException {
        if (null != conn) {
            conn.close();
        }
    }
}
