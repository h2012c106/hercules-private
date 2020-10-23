package com.xiaohongshu.db.hercules.elasticsearch.schema.manager;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public class DocRequest {
    public String index;
    public String type;
    public String id;
    public String doc;

    public DocRequest(String index, String type, String id, String doc) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.doc = doc;
    }

    public Map<String, Object> getDoc() {
        return JSONObject.parseObject(doc).getInnerMap();
    }
}
