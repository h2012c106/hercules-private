package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import lombok.SneakyThrows;

import java.sql.*;
import java.util.*;

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

        Class.forName ("com.mysql.cj.jdbc.Driver");
//        String url = "jdbc:mysql://10.23.145.1:3306/metastore";
        String url = getOptions().getString(HBaseOptionsConf.HIVE_METASTORE_URL, "");
        if(url.equals("")){
            return null;
        }

        String hiveUser = getOptions().getString(HBaseOptionsConf.HIVE_USER, "");
        String hivePasswd = getOptions().getString(HBaseOptionsConf.HIVE_PASSWD, "");
        String hiveTable = getOptions().getString(HBaseOptionsConf.HIVE_TABLE, "");

        Properties props = new Properties();
        props.setProperty("user",hiveUser);
        props.setProperty("password",hivePasswd);

        Connection conn = DriverManager.getConnection(url,props);
        Statement statement = conn.createStatement();
        // metastore 中可能存在重复的table名字
        String sql =String.format("select COLUMN_NAME,TYPE_NAME from TBLS t JOIN SDS s ON t.SD_ID = s.SD_ID JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID where " +
                "t.TBL_NAME='%s' and t.TBL_ID=(select TBL_ID from TBLS t where t.TBL_NAME='%s' limit 1);", hiveTable);
        ResultSet resultSet = statement.executeQuery(sql);

        Map<String, DataType> columnTypeMap = new HashMap<>();
        while(resultSet.next()){
            String columnName = resultSet.getString(1);
            if(columnNameSet.contains(columnName)||columnNameSet.size()==0){
                columnTypeMap.put(columnName,converter.hbaseConvertElementType(resultSet.getString(2)));
            }
        }
        return columnTypeMap;
    }
}
