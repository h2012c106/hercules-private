package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import lombok.SneakyThrows;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseSchemaFetcher extends BaseSchemaFetcher<HBaseDataTypeConverter> {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private String url;
    private String hiveUser;
    private String hivePasswd;
    private String hiveTable;
    private Connection conn;

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
    @SneakyThrows
    @Override
    protected Map<String, DataType> innerGetColumnTypeMap(Set<String> columnNameSet) {

        Map columnTypeMap = new HashMap<String, DataType>();
        url = getOptions().getString(HBaseOptionsConf.HIVE_URL, "");
        if(url.equals("")){
            return columnTypeMap;
        }
        Class.forName(driverName);
        // Establish Hive connection and try to get schema if url is given
        hiveUser = getOptions().getString(HBaseOptionsConf.HIVE_USER, "");
        hivePasswd = getOptions().getString(HBaseOptionsConf.HIVE_PASSWD, "");
        hiveTable = getOptions().getString(HBaseOptionsConf.HIVE_Table, getOptions().getString(HBaseOptionsConf.TABLE, ""));
        if(!url.equals("")){
            try{
                conn = DriverManager.getConnection(url, hiveUser, hivePasswd);
                DatabaseMetaData databaseMetaData = conn.getMetaData();
                ResultSet columns = databaseMetaData.getColumns(null,null,hiveTable,null);
                while(columns.next()){
                    String columnName = columns.getString("COLUMN_NAME");
                    String datatype = columns.getString("DATA_TYPE");
//                    String columnsize = columns.getString("COLUMN_SIZE");
//                    String decimaldigits = columns.getString("DECIMAL_DIGITS");
//                    String isNullable = columns.getString("IS_NULLABLE");
//                    String is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
                    columnTypeMap.put(columnName, datatype);
                }
            }catch(SQLException e){
                throw e;
            }
        }
        return columnTypeMap;
    }

    public Map<String, DataType> getColumnListFromHive(){
        return null;

    }
}
