package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.hbase.option.HBaseOptionsConf;
import lombok.SneakyThrows;

import java.sql.*;
import java.util.*;

public class HBaseSchemaFetcher extends BaseSchemaFetcher {

    private final List<String> columnNameList = new ArrayList<>();
    private final Map<String, DataType> columnTypeMap = new HashMap<>();

    @Assembly
    private HBaseDataTypeConverter dataTypeConverter;

    @SneakyThrows
    public HBaseSchemaFetcher(GenericOptions options) {
        super(options);
//        initHiveMetaInfo();
    }

    // 如果 HBase 的 ColumnTypeMap 由用户全部指定，不需要从数据库中获取。
    private void initHiveMetaInfo() throws SQLException, ClassNotFoundException {
        String url = getOptions().getString(HBaseOptionsConf.HIVE_METASTORE_URL, "");
        if (url.equals("")) {
            return;
        }
        Class.forName("com.mysql.cj.jdbc.Driver");
        String hiveUser = getOptions().getString(HBaseOptionsConf.HIVE_USER, "");
        String hivePasswd = getOptions().getString(HBaseOptionsConf.HIVE_PASSWD, "");
        String hiveTable = getOptions().getString(HBaseOptionsConf.HIVE_TABLE, "");

        Properties props = new Properties();
        props.setProperty("user", hiveUser);
        props.setProperty("password", hivePasswd);

        Connection conn = DriverManager.getConnection(url, props);
        Statement statement = conn.createStatement();
        // metastore 中可能存在重复的table名字
        String sql = String.format("select COLUMN_NAME,TYPE_NAME from TBLS t JOIN SDS s ON t.SD_ID = s.SD_ID JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID where " +
                "t.TBL_NAME='%s' and t.TBL_ID=(select TBL_ID from TBLS t where t.TBL_NAME='%s' limit 1);", hiveTable, hiveTable);
        ResultSet resultSet = statement.executeQuery(sql);

        while (resultSet.next()) {
            String columnName = resultSet.getString(1);
            columnTypeMap.put(columnName, dataTypeConverter.hbaseConvertElementType(resultSet.getString(2)));
            columnNameList.add(columnName);
        }
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return columnNameList;
    }

    @Override
    public Map<String, DataType> innerGetColumnTypeMap() {
        return columnTypeMap;
    }

    @Override
    protected List<Set<String>> innerGetIndexGroupList() {
        return new LinkedList<>();
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        return new LinkedList<>();
    }
}
