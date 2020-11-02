package com.xiaohongshu.db.hercules.elasticsearchv7.schema.manager;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public class DocRequest {
    public String index;
    public String id;
    public String doc;

    public DocRequest(String index, String id, String doc) {
        this.index = index;
        this.id = id;
        this.doc = doc;
    }

    public Map<String, Object> getDoc() {
        return JSONObject.parseObject(doc).getInnerMap();
    }
}
