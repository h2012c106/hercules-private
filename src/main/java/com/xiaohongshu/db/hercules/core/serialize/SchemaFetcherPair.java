package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.DataSourceRole;

/**
 * 用于存储source、target的SchemaFetcher
 */
public class SchemaFetcherPair {

    private static BaseSchemaFetcher sourceSchemaFetcher;
    private static BaseSchemaFetcher targetSchemaFetcher;

    synchronized public static void set(BaseSchemaFetcher schemaFetcher, DataSourceRole role) {
        switch (role) {
            case SOURCE:
                sourceSchemaFetcher = schemaFetcher;
                break;
            case TARGET:
                targetSchemaFetcher = schemaFetcher;
                break;
            default:
                throw new RuntimeException("Unknown data source role: " + role);
        }
    }

    public static BaseSchemaFetcher get(DataSourceRole role) {
        BaseSchemaFetcher res = null;
        switch (role) {
            case SOURCE:
                res = sourceSchemaFetcher;
                break;
            case TARGET:
                res = targetSchemaFetcher;
                break;
            default:
                throw new RuntimeException("Unknown data source role: " + role);
        }
        if (res == null) {
            throw new RuntimeException("Unset schema fetcher.");
        }
        return res;
    }
}
