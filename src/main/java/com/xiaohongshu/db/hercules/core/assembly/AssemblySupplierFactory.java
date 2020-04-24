package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.clickhouse.ClickhouseAssemblySupplier;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.tidb.TiDBAssemblySupplier;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public final class AssemblySupplierFactory {
    private static final Map<DataSource, Class<? extends BaseAssemblySupplier>> REGISTER_CENTER
            = new HashMap<>(DataSource.values().length);

    static {
        //注册
        // register(aaa, xxx.class)
        register(DataSource.RDBMS, RDBMSAssemblySupplier.class);
        register(DataSource.MySQL, MysqlAssemblySupplier.class);
        register(DataSource.TiDB, TiDBAssemblySupplier.class);
        register(DataSource.Clickhouse, ClickhouseAssemblySupplier.class);
    }

    private static void register(DataSource dataSource, Class<? extends BaseAssemblySupplier> assemblySupplierClass) {
        if (REGISTER_CENTER.containsKey(dataSource)) {
            throw new RuntimeException(String.format("Duplicate assembly supplier register of %s",
                    dataSource.name()));
        } else {
            REGISTER_CENTER.put(dataSource, assemblySupplierClass);
        }
    }

    public static BaseAssemblySupplier getAssemblySupplier(DataSource dataSource, GenericOptions options) {
        if (REGISTER_CENTER.containsKey(dataSource)) {
            Class<? extends BaseAssemblySupplier> assemblySupplierClass = REGISTER_CENTER.get(dataSource);
            try {
                Constructor<? extends BaseAssemblySupplier> constructor
                        = assemblySupplierClass.getConstructor(GenericOptions.class);
                boolean accessible = constructor.isAccessible();
                constructor.setAccessible(true);
                BaseAssemblySupplier res = constructor.newInstance(options);
                constructor.setAccessible(accessible);
                return res;
            } catch (Exception e) {
                throw new ParseException(e);
            }
        } else {
            throw new ParseException(String.format("Unsupported data source %s", dataSource.name()));
        }
    }
}
