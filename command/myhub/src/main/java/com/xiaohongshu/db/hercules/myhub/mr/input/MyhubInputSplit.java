package com.xiaohongshu.db.hercules.myhub.mr.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MyhubInputSplit extends InputSplit implements Writable {

    private final List<Integer> shardSeqList;

    public MyhubInputSplit() {
        shardSeqList = new ArrayList<>();
    }

    public MyhubInputSplit(int seq) {
        shardSeqList = Collections.singletonList(seq);
    }

    public void add(int seq) {
        shardSeqList.add(seq);
    }

    public List<Integer> getShardSeqList() {
        return shardSeqList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(shardSeqList.size());
        for (int seq : shardSeqList) {
            out.writeInt(seq);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            shardSeqList.add(in.readInt());
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }
}
