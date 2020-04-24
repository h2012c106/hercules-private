package com.xiaohongshu.db.hercules.hbase.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase2.MangerFactory.BaseManager;
import com.xiaohongshu.db.hercules.hbase2.option.HbaseOptionsConf;
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

public class HBaseManager extends BaseManager {

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

    public void setConf(){
        // set connection
        conf.set(HbaseOptionsConf.HB_ZK_QUORUM, options.getString(HbaseOptionsConf.HB_ZK_QUORUM,null));
        conf.set(HbaseOptionsConf.HB_ZK_PORT, options.getString(HbaseOptionsConf.HB_ZK_PORT,"2182"));
    }

    public Configuration getConf(){
        // return conf if conf already set
        if(conf != null){
            return conf;
        }
        conf = new Configuration();
        setConf();
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
}
