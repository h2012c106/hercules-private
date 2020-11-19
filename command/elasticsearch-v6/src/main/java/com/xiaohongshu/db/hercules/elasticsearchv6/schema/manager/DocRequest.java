package com.xiaohongshu.db.hercules.elasticsearchv6.schema.manager;

import org.bson.Document;

import java.util.Map;

public class DocRequest {
    public String index;
    public String id;
    public Document doc;

    public DocRequest(String index, String id, Document doc) {
        this.index = index;
        this.id = id;
        this.doc = doc;
    }

    public Map<String, Object> getDoc() {
        return doc;
    }
}
