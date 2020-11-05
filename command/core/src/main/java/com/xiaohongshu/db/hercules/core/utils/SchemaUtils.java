package com.xiaohongshu.db.hercules.core.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.HiveMetaOptionsConf.*;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf.COLUMN_DELIMITER;

public final class SchemaUtils {

    /**
     * 获得目标列列表在源列表的下标，若目标列表多列，值为null
     *
     * @param columnMap
     * @return
     */
    public static List<Integer> mapColumnSeq(List<String> sourceColumnList,
                                             List<String> targetColumnList,
                                             JSONObject columnMap) {
        sourceColumnList = sourceColumnList.stream()
                .map(columnName -> columnMap.getInnerMap().getOrDefault(columnName, columnName).toString())
                .collect(Collectors.toList());
        Map<String, Integer> sourceNameToSeq = new HashMap<>();
        for (int i = 0; i < sourceColumnList.size(); ++i) {
            sourceNameToSeq.put(sourceColumnList.get(i), i);
        }
        List<Integer> res = new ArrayList<>(targetColumnList.size());
        for (String columnName : targetColumnList) {
            res.add(sourceNameToSeq.getOrDefault(columnName, null));
        }
        return res;
    }

    public static BiMap<String, String> convertColumnMapFromOption(@NonNull JSONObject jsonObject) {
        // 源列名->目标列名
        BiMap<String, String> biColumnMap = HashBiMap.create(jsonObject.size());
        for (String key : jsonObject.keySet()) {
            // 如果存在相同的value则bimap会报错
            biColumnMap.put(key.trim(), jsonObject.getString(key).trim());
        }
        return biColumnMap;
    }

    public static List<String> convertNameFromOption(@NonNull String[] names) {
        return Arrays.stream(names).map(String::trim).filter(item -> item.length() > 0).collect(Collectors.toList());
    }

    public static Map<String, DataType> convertTypeFromOption(@NonNull JSONObject jsonObject) {
        return convertTypeFromOption(jsonObject, NullCustomDataTypeManager.INSTANCE);
    }

    public static Map<String, DataType> convertTypeFromOption(@NonNull JSONObject jsonObject, final CustomDataTypeManager<?, ?> manager) {
        return jsonObject.getInnerMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().trim(),
                        entry -> DataType.valueOfIgnoreCase((String) entry.getValue(), manager)));
    }

    public static JSONObject convertTypeToOption(@NonNull Map<String, DataType> map) {
        JSONObject res = new JSONObject();
        for (Map.Entry<String, DataType> entry : map.entrySet()) {
            res.put(entry.getKey(), entry.getValue().getName());
        }
        return res;
    }

    public static List<Set<String>> convertIndexFromOption(@NonNull String[] array) {
        return convertIndexFromOption(Arrays.asList(array));
    }

    public static List<Set<String>> convertIndexFromOption(@NonNull List<String> list) {
        List<Set<String>> res = new ArrayList<>(list.size());
        for (String group : list.stream().map(String::trim).filter(item -> item.length() > 0).collect(Collectors.toList())) {
            Set<String> groupSet = Arrays.stream(StringUtils.split(group, COLUMN_DELIMITER))
                    .map(String::trim)
                    .filter(item -> item.length() > 0)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            if (groupSet.size() > 0) {
                res.add(groupSet);
            }
        }
        return res;
    }

    public static String[] convertIndexToOption(List<Set<String>> list) {
        List<String> res = new ArrayList<>(list.size());
        for (Set<String> group : list) {
            res.add(StringUtils.join(group, COLUMN_DELIMITER));
        }
        return res.toArray(new String[0]);
    }

    /**
     * 把顺序的索引列表按照最左匹配原则展开，适用于Mysql、Mongo
     *
     * @param indexList
     * @return
     */
    public static List<Set<String>> unwrapIndexList(List<String> indexList) {
        List<Set<String>> res = new ArrayList<>(indexList.size());
        for (int i = 1; i <= indexList.size(); ++i) {
            res.add(new LinkedHashSet<>(indexList.subList(0, i)));
        }
        return res;
    }

    private static void registerDriver(String driverClassName) {
        try {
            registerDriver((Class<? extends Driver>) Class.forName(driverClassName, true, Thread.currentThread().getContextClassLoader()));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerDriver(Class<? extends Driver> driverClass) {
        try {
            DriverManager.registerDriver(new ClassLoaderLocalizedDriver(driverClass.newInstance()));
        } catch (SQLException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> fetchHiveTableSchema(@NonNull String url,
                                                           @NonNull String user, @NonNull String password,
                                                           @NonNull String driver,
                                                           @NonNull String database, @NonNull String table) {
        String sql = "SELECT `COLUMN_NAME`, `TYPE_NAME` FROM `COLUMNS_V2` WHERE `CD_ID` = (SELECT `CD_ID` FROM `SDS` WHERE `SD_ID` = (SELECT `SD_ID` FROM `TBLS` WHERE `TBL_NAME` = ? AND `DB_ID` = (SELECT `DB_ID` FROM `DBS` WHERE `NAME` = ?))) ORDER BY `INTEGER_IDX`;";

        registerDriver(driver);
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.prepareStatement(sql);
            statement.setString(1, table);
            statement.setString(2, database);
            resultSet = statement.executeQuery();
            Map<String, String> res = new LinkedHashMap<>();
            while (resultSet.next()) {
                res.put(resultSet.getString(1), resultSet.getString(2));
            }
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException ignore) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ignore) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    public static Map<String, String> fetchHiveTableSchema(GenericOptions options) {
        String url = options.getString(HIVE_META_CONNECTION, null);
        String user = options.getString(HIVE_META_USER, null);
        String password = options.getString(HIVE_META_PASSWORD, null);
        String driver = options.getString(HIVE_META_DRIVER, null);
        String database = options.getString(HIVE_DATABASE, null);
        String table = options.getString(HIVE_TABLE, null);
        return fetchHiveTableSchema(url, user, password, driver, database, table);
    }

    /**
     * 由于DriverManager对非本类加载器注册的Driver不予以使用，这个类Localize一下
     */
    private static class ClassLoaderLocalizedDriver implements Driver {
        private final Driver delegate;

        private ClassLoaderLocalizedDriver(Driver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            return delegate.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return delegate.acceptsURL(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return delegate.getPropertyInfo(url, info);
        }

        @Override
        public int getMajorVersion() {
            return delegate.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return delegate.getMinorVersion();
        }

        @Override
        public boolean jdbcCompliant() {
            return delegate.jdbcCompliant();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }
    }
}
