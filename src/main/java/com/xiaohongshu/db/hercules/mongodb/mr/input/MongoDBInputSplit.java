package com.xiaohongshu.db.hercules.mongodb.mr.input;

import com.mongodb.BasicDBObjectBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MongoDBInputSplit extends InputSplit implements Writable {

    private Document splitQuery;

    private transient BSONEncoder _bsonEncoder = new BasicBSONEncoder();
    private transient BSONDecoder _bsonDecoder = new BasicBSONDecoder();

    public MongoDBInputSplit() {
    }

    public MongoDBInputSplit(Object min, Object max, String splitBy) {
        splitQuery = new Document();
        if (min != null || max != null) {
            Document rangeQuery = new Document();
            if (min != null) {
                rangeQuery.put("$gte", min);
            }
            if (max != null) {
                rangeQuery.put("$lt", max);
            }
            splitQuery.put(splitBy, rangeQuery);
        }
    }

    public Document getSplitQuery() {
        return splitQuery;
    }

    public void setSplitQuery(Document splitQuery) {
        this.splitQuery = splitQuery;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        BSONObject spec = BasicDBObjectBuilder.start()
                .add("splitQuery", getSplitQuery())
                .get();
        byte[] buf = _bsonEncoder.encode(spec);
        out.write(buf);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        BSONCallback cb = new BasicBSONCallback();
        BSONObject spec;
        byte[] l = new byte[4];
        in.readFully(l);
        int dataLen = org.bson.io.Bits.readInt(l);
        byte[] data = new byte[dataLen + 4];
        System.arraycopy(l, 0, data, 0, 4);
        in.readFully(data, 4, dataLen - 4);
        _bsonDecoder.decode(data, cb);
        spec = (BSONObject) cb.get();

        BSONObject temp;

        temp = (BSONObject) spec.get("splitQuery");
        setSplitQuery(temp != null ? new Document(temp.toMap()) : null);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public String toString() {
        return splitQuery.toString();
    }
}
