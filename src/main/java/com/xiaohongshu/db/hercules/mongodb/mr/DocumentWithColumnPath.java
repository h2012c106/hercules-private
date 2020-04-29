package com.xiaohongshu.db.hercules.mongodb.mr;


import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

public class DocumentWithColumnPath {
    private Document document;
    /**
     * {@link #document}所在位置的完整列名
     */
    private String columnPath;

    public DocumentWithColumnPath(Document document, String columnPath) {
        this.document = document;
        this.columnPath = columnPath;
    }

    public Document getDocument() {
        return document;
    }

    public String getColumnPath() {
        return columnPath;
    }
}
