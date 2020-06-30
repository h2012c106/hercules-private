package com.xiaohongshu.db.hercules.rdbms.mr.output.statement;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.rdbms.ExportType;

public final class StatementGetterFactory {

    public static StatementGetter get(ExportType exportType) {
        switch (exportType) {
            case INSERT:
                return new InsertStatementGetter();
            case INSERT_IGNORE:
                return new InsertIgnoreStatementGetter();
            case REPLACE:
                return new ReplaceStatementGetter();
            case UPSERT:
                return new UpsertStatementGetter();
            case UPDATE:
                return new UpdateStatementGetter();
            default:
                throw new MapReduceException("Unknown export type: " + exportType.name());
        }
    }
}
