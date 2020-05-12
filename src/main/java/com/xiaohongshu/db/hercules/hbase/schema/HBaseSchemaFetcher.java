package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseSchemaFetcher extends BaseSchemaFetcher<HBaseDataTypeConverter> {

    public HBaseSchemaFetcher(GenericOptions options, HBaseDataTypeConverter converter) {
        super(options, converter);
    }

    /**
     *  目前HBase的列名列表依靠用户输入，这个类是为了保证与框架统一
     */
    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    // 如果 HBase 的 ColumnTypeMap 由用户全部指定，不需要从数据库中获取。

    /**
     * Connect to hive metastore and retrieve ColumnTypeMap
     */
    @SneakyThrows
    @Override
    public Map<String, DataType> innerGetColumnTypeMap(Set<String> columnNameSet) {

        String dbName = getOptions().getString(HBaseOptionsConf.HIVE_DATABASE, "");
        String tbName = getOptions().getString(HBaseOptionsConf.HIVE_TABLE, "");
        String thriftUrl = getOptions().getString(HBaseOptionsConf.HIVE_THRIFT_URL, "");
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", thriftUrl);
        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, false);
        // 不兼容时的设置(可能需要)
//        client.setMetaConf("hive.metastore.client.capability.check","false");
        List<FieldSchema> schema = client.getSchema(dbName, tbName);
        HashMap<String, DataType> columnTypeMap = new HashMap();
        for(FieldSchema fs:schema){
            String columnName = fs.getName();
            // TODO 若columnNameSet为空，则全部传出去 (确定这里的逻辑)
            if(columnNameSet.contains(columnName)||columnNameSet.size()==0){
                columnTypeMap.put(columnName, converter.hbaseConvertElementType(fs.getType()));
            }
        }
        return columnTypeMap;
    }


    @SneakyThrows
    protected Map<String, DataType> getColumnListFromHive(Set<String> columnNameSet) {

        Map<String, DataType> columnTypeMap = new HashMap<String, DataType>();
        String url = getOptions().getString(HBaseOptionsConf.HIVE_URL, "");
        if(url.equals("")){
            return columnTypeMap;
        }
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driverName);
        // Establish Hive connection and try to get schema if url is given
        String hiveUser = getOptions().getString(HBaseOptionsConf.HIVE_USER, "");
        String hivePasswd = getOptions().getString(HBaseOptionsConf.HIVE_PASSWD, "");
        String hiveTable = getOptions().getString(HBaseOptionsConf.HIVE_TABLE, getOptions().getString(HBaseOptionsConf.TABLE, ""));
        if(!url.equals("")){
            try{
                Connection conn = DriverManager.getConnection(url, hiveUser, hivePasswd);
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet columns = databaseMetaData.getColumns(null,null, hiveTable,null);
                while(columns.next()){
                    String columnName = columns.getString("COLUMN_NAME");
                    String datatype = columns.getString("DATA_TYPE");
//                    String columnsize = columns.getString("COLUMN_SIZE");
//                    String decimaldigits = columns.getString("DECIMAL_DIGITS");
//                    String isNullable = columns.getString("IS_NULLABLE");
//                    String is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
                    columnTypeMap.put(columnName, DataType.valueOf(datatype));
                }
            }catch(SQLException e){
                throw e;
            }
        }
        return columnTypeMap;
    }

}
