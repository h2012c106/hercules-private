package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exceptions.ParseException;

import java.util.HashMap;
import java.util.Map;

public class ParserFactory {

    private static Map<DataSource, Map<DataSourceRole, BaseDataSourceParser>> registerCenter
            = new HashMap<DataSource, Map<DataSourceRole, BaseDataSourceParser>>(DataSource.values().length);

    static {
        // 类似这么注册
        // register(new xxx());
    }

    private static void register(BaseDataSourceParser instance) {
        DataSource dataSource = instance.getDataSource();
        DataSourceRole dataSourceRole = instance.getDataSourceRole();
        if (registerCenter.containsKey(dataSource)) {
            Map<DataSourceRole, BaseDataSourceParser> dataSourceRoleBaseDataSourceParserMap = registerCenter.get(dataSource);
            // 不允许在同一串key上重复注册，防止写组件的时候无脑复制粘贴导致注册的时候张冠李戴
            if (dataSourceRoleBaseDataSourceParserMap.containsKey(dataSourceRole)) {
                throw new RuntimeException(String.format("Duplicate parser register of %s as %s role",
                        dataSource.name(),
                        dataSourceRole.name()));
            } else {
                dataSourceRoleBaseDataSourceParserMap.put(dataSourceRole, instance);
            }
        } else {
            Map<DataSourceRole, BaseDataSourceParser> dataSourceRoleBaseDataSourceParserMap
                    = new HashMap<DataSourceRole, BaseDataSourceParser>(2);
            dataSourceRoleBaseDataSourceParserMap.put(dataSourceRole, instance);
            registerCenter.put(dataSource, dataSourceRoleBaseDataSourceParserMap);
        }
    }

    public static BaseDataSourceParser getParser(DataSource dataSource, DataSourceRole dataSourceRole) {
        if (registerCenter.containsKey(dataSource)) {
            Map<DataSourceRole, BaseDataSourceParser> dataSourceRoleBaseDataSourceParserMap = registerCenter.get(dataSource);
            if (dataSourceRoleBaseDataSourceParserMap.containsKey(dataSourceRole)) {
                return dataSourceRoleBaseDataSourceParserMap.get(dataSourceRole);
            } else {
                throw new ParseException(String.format("Unsupported data source %s as %s",
                        dataSource.name(),
                        dataSourceRole.name()));
            }
        } else {
            throw new ParseException(String.format("Unsupported data source %s",
                    dataSource.name()));
        }
    }
}
