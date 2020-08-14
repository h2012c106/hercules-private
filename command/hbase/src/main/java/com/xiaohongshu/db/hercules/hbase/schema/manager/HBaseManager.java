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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseManager {

    private static final Log LOG = LogFactory.getLog(HBaseManager.class);

    private final GenericOptions options;
    private volatile Connection conn;

    public HBaseManager(GenericOptions options) {
        this.options = options;
    }

    public Connection getConnection(Configuration configuration) throws IOException {
        if (null == conn) {
            synchronized (Connection.class) {
                if (null == conn) {
                    conn = ConnectionFactory.createConnection(configuration);
                }
            }
        }
        return conn;
    }

    public void closeConnection() throws IOException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public static List<RegionInfo> getRegionInfo(Connection connection, String tn) throws IOException {
        Admin admin = connection.getAdmin();
        List<RegionInfo> rsInfo = admin.getRegions(TableName.valueOf(tn));
        admin.close();
        return rsInfo;
    }

    public static Table getHtable(Connection connection, String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static Scan genScan(Scan scan, String startKey, String endKey, GenericOptions options) throws IOException {
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
        List<String> scanColumns = Arrays.asList(options.getTrimmedStringArray(BaseDataSourceOptionsConf.COLUMN, new String[0]));
        if (scanColumns.size() != 0) {
            for (String qualifier : scanColumns) {
                scan.addColumn(Bytes.toBytes(scanColumnFamily), Bytes.toBytes(qualifier));
            }
        }
        // cache blocks 配置项不开放给用户
        scan.setCacheBlocks(false);
        return scan;
    }

    public static void setBasicConf(Configuration conf, GenericOptions targetOptions) {
        conf.set("hbase.zookeeper.quorum", targetOptions.getString(HBaseOptionsConf.HB_ZK_QUORUM, null));
        conf.set("hbase.zookeeper.property.clientPort", targetOptions.getString(HBaseOptionsConf.HB_ZK_PORT, "2181"));
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    public static void setTargetConf(Configuration conf, GenericOptions targetOptions) {
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

    public static Table getTable(String tableName, Connection connection) throws IOException {
        TableName hTableName = TableName.valueOf(tableName);
        return connection.getTable(hTableName);
    }

    /**
     * 通过配置好的conf以及manager来获取BufferedMutator(Async).
     */
    public static BufferedMutator getBufferedMutator(GenericOptions options, Connection connection, Configuration configuration) throws IOException {
        String userTable = options.getString(HBaseOptionsConf.TABLE, null);
        long writeBufferSize = options.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, HBaseOutputOptionsConf.DEFAULT_WRITE_BUFFER_SIZE);
        TableName hTableName = TableName.valueOf(userTable);
        Admin admin = null;
        BufferedMutator bufferedMutator;
        try {
            admin = connection.getAdmin();
            bufferedMutator = connection.getBufferedMutator(
                    new BufferedMutatorParams(hTableName)
                            .pool(HTable.getDefaultExecutor(configuration))
                            .writeBufferSize(writeBufferSize));
        } catch (Exception e) {
            closeAdmin(admin);
            closeConnection(connection);
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
