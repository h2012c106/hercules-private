package com.xiaohongshu.db.hercules.converter.mongo;

import com.alibaba.fastjson.annotation.JSONField;

import java.time.LocalDateTime;

public class OplogRecord {
    private String doc;
    @JSONField(name="raw_oplog")
    private String rawOplog;

    private String optype;
    @JSONField(format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", name="event_time")
    private LocalDateTime eventTime;

    public OplogRecord() {
    }

    public OplogRecord(String doc, String rawOplog, String optype, LocalDateTime eventTime) {
        this.doc = doc;
        this.rawOplog = rawOplog;
        this.optype = optype;
        this.eventTime = eventTime;
    }

    public String getRawOplog() {
        return rawOplog;
    }

    public void setRawOplog(String rawOplog) {
        this.rawOplog = rawOplog;
    }

    public String getDoc() {
        return doc;
    }

    public void setDoc(String doc) {
        this.doc = doc;
    }

    public String getOptype() {
        return optype;
    }

    public void setOptype(String optype) {
        this.optype = optype;
    }

    public LocalDateTime getEventTime() {
        return eventTime;
    }

    public void setEventTime(LocalDateTime eventTime) {
        this.eventTime = eventTime;
    }
}
