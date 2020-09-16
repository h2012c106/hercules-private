package com.xiaohongshu.db.hercules.serder.json.ser;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.mongodb.mr.output.MongoDBWrapperSetterManager;
import org.bson.Document;


/**
 * 基于 MongoDBWrapperSetterManager/Document
 * 不支持bytes和longlong
 */
public class JsonWrapperSetterManager extends MongoDBWrapperSetterManager {

    public JsonWrapperSetterManager() {
        setDecimalAsString(true);
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<Document> getDateSetter() {
        return new BaseTypeWrapperSetter.DateSetter<Document>() {
            @Override
            protected void setNonnullValue(ExtendedDate value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, new java.sql.Date(value.getDate().getTime()).toString());
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<Document> getTimeSetter() {
        return new BaseTypeWrapperSetter.TimeSetter<Document>() {
            @Override
            protected void setNonnullValue(ExtendedDate value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, new java.sql.Date(value.getDate().getTime()).toString());
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<Document> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<Document>() {

            @Override
            protected void setNonnullValue(ExtendedDate value, Document row, String rowName, String columnName, int columnSeq) throws Exception {
                if (value.isZero()) {
                    row.put(columnName, DateUtils.ZERO_DATE);
                } else {
                    row.put(columnName, new java.sql.Date(value.getDate().getTime()).toString());
                }
            }

            @Override
            protected void setNull(Document row, String rowName, String columnName, int columnSeq) throws Exception {
                row.put(columnName, null);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<Document> getBytesSetter() {
        return null;
    }
}
