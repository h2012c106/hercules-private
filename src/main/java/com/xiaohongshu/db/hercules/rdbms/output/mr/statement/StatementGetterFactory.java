package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

import com.xiaohongshu.db.hercules.core.exceptions.MapReduceException;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;

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
