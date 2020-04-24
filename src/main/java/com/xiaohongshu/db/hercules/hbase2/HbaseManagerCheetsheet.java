package com.xiaohongshu.db.hercules.hbase2;

import com.xiaohongshu.db.hercules.hbase2.MangerFactory.BaseManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseManagerCheetsheet extends BaseManager {

    // 声明静态配置
    private Configuration conf;
    private Connection conn;

    public HbaseManagerCheetsheet(Configuration conf, String tableName) {
        this.conf = conf;
    }

    // create connection
    public Connection getConnection() throws IOException {
         if(conn==null||conn.isClosed()){
            conn = ConnectionFactory.createConnection(conf);
         }
         return conn;
    }

    // get a row of data
    public Result getSinlgeRowData(byte[] rowKey, HTable table) throws IOException {
        Get get = new Get(rowKey);
        Result res = table.get(get);
        
        return res;
    }

    // get a set of data (scan)
    public ResultScanner getScannerByStartStopKeys(byte[] startKey, byte[] stopKey, HTable table) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(startKey)
                .withStopRow(stopKey);
        ResultScanner scanner = table.getScanner(scan);
        return scanner;
    }

    public void putSingleRowData(byte[] rowKey, byte[] columnFamily,
                                 List<byte[]> columnNames, List<byte[]> values,
                                 HTable table) throws IOException {
        Put p = new Put(rowKey);
        for(int i=0;i<values.size();i++){
            p.addImmutable(columnFamily, columnNames.get(i), values.get(i));
            table.put(p);
        }
    }

    public void deleteSingleRowData(byte[] rowKey, HTable table) throws IOException {
        Delete del = new Delete(rowKey);
        table.delete(del);
    }

    public void createTable(String tn, byte[] columnFamily) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tn));
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        table.addFamily(family);
        admin.createTable(table);
        admin.close();
    }

    /**
     * Creates a new table. Synchronous operation.
     *
     * @param tn String, table name
     * @return rsInfo, List<RegionInfo>
     */
    public List<RegionInfo> getRegionInfo(String tn) throws IOException {
        Admin admin = conn.getAdmin();
        List<RegionInfo> rsInfo = admin.getRegions(TableName.valueOf(tn));
        admin.close();
        return rsInfo;
    }

    public List<TableDescriptor> getTableDescriptorsList() throws IOException {

        Connection conn = getConnection();
        Admin admin = conn.getAdmin();
        List<TableDescriptor> res = admin.listTableDescriptors();
        admin.close();
        return res;
    }
}
