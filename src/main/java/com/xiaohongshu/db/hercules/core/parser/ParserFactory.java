package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.clickhouse.parser.ClickhouseInputParser;
import com.xiaohongshu.db.hercules.clickhouse.parser.ClickhouseOutputParser;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.hbase.parser.HBaseInputParser;
import com.xiaohongshu.db.hercules.hbase.parser.HBaseOutputParser;
import com.xiaohongshu.db.hercules.mongodb.parser.MongoDBInputParser;
import com.xiaohongshu.db.hercules.mongodb.parser.MongoDBOutputParser;
import com.xiaohongshu.db.hercules.myhub.option.MyhubOutputOptionsConf;
import com.xiaohongshu.db.hercules.mysql.option.MysqlInputOptionsConf;
import com.xiaohongshu.db.hercules.mysql.parser.MysqlInputParser;
import com.xiaohongshu.db.hercules.mysql.parser.MysqlOutputParser;
import com.xiaohongshu.db.hercules.parquet.parser.ParquetInputParser;
import com.xiaohongshu.db.hercules.parquet.parser.ParquetOutputParser;
import com.xiaohongshu.db.hercules.parquetschema.parser.ParquetSchemaParser;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSInputParser;
import com.xiaohongshu.db.hercules.rdbms.parser.RDBMSOutputParser;
import com.xiaohongshu.db.hercules.tidb.parser.TiDBInputParser;

import java.util.HashMap;
import java.util.Map;

public final class ParserFactory {

    private static final Map<DataSource, Map<DataSourceRole, BaseParser>> REGISTER_CENTER
            = new HashMap<DataSource, Map<DataSourceRole, BaseParser>>(DataSource.values().length);

    static {
        register(DataSource.RDBMS, DataSourceRole.SOURCE, new RDBMSInputParser());
        register(DataSource.RDBMS, DataSourceRole.TARGET, new RDBMSOutputParser());
        register(DataSource.MySQL, DataSourceRole.SOURCE, new MysqlInputParser());
        register(DataSource.MySQL, DataSourceRole.TARGET, new MysqlOutputParser());
        register(DataSource.TiDB, DataSourceRole.SOURCE, new TiDBInputParser());
        register(DataSource.TiDB, DataSourceRole.TARGET, new MysqlOutputParser());
        register(DataSource.Clickhouse, DataSourceRole.SOURCE, new ClickhouseInputParser());
        register(DataSource.Clickhouse, DataSourceRole.TARGET, new ClickhouseOutputParser());
        register(DataSource.HBase, DataSourceRole.SOURCE, new HBaseInputParser());
        register(DataSource.HBase, DataSourceRole.TARGET, new HBaseOutputParser());
        register(DataSource.MongoDB, DataSourceRole.SOURCE, new MongoDBInputParser());
        register(DataSource.MongoDB, DataSourceRole.TARGET, new MongoDBOutputParser());
        register(DataSource.Parquet, DataSourceRole.SOURCE, new ParquetInputParser());
        register(DataSource.Parquet, DataSourceRole.TARGET, new ParquetOutputParser());
        register(DataSource.ParquetSchema, DataSourceRole.TARGET, new ParquetSchemaParser());
        register(DataSource.Myhub, DataSourceRole.SOURCE, new BaseParser(new MysqlInputOptionsConf(), DataSource.Myhub, DataSourceRole.SOURCE));
        // register(DataSource.Myhub, DataSourceRole.TARGET, new BaseParser(new MyhubOutputOptionsConf(), DataSource.Myhub, DataSourceRole.TARGET));
    }

    private static void register(DataSource dataSource, DataSourceRole dataSourceRole, BaseParser instance) {
        if (REGISTER_CENTER.containsKey(dataSource)) {
            Map<DataSourceRole, BaseParser> dataSourceRoleBaseDataSourceParserMap = REGISTER_CENTER.get(dataSource);
            // 不允许在同一串key上重复注册，防止写组件的时候无脑复制粘贴导致注册的时候张冠李戴
            if (dataSourceRoleBaseDataSourceParserMap.containsKey(dataSourceRole)) {
                throw new RuntimeException(String.format("Duplicate parser register of %s as %s role",
                        dataSource.name(),
                        dataSourceRole.name()));
            } else {
                dataSourceRoleBaseDataSourceParserMap.put(dataSourceRole, instance);
            }
        } else {
            Map<DataSourceRole, BaseParser> dataSourceRoleBaseDataSourceParserMap
                    = new HashMap<DataSourceRole, BaseParser>(2);
            dataSourceRoleBaseDataSourceParserMap.put(dataSourceRole, instance);
            REGISTER_CENTER.put(dataSource, dataSourceRoleBaseDataSourceParserMap);
        }
    }

    public static BaseParser getParser(DataSource dataSource, DataSourceRole dataSourceRole) {
        if (REGISTER_CENTER.containsKey(dataSource)) {
            Map<DataSourceRole, BaseParser> dataSourceRoleBaseDataSourceParserMap = REGISTER_CENTER.get(dataSource);
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
