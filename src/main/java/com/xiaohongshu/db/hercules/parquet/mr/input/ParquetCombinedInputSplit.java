package com.xiaohongshu.db.hercules.parquet.mr.input;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.hadoop.ParquetInputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ParquetCombinedInputSplit extends InputSplit implements Writable {

    private final List<FileSplit> combinedInputSplitList;

    public ParquetCombinedInputSplit() {
        this.combinedInputSplitList = new ArrayList<>();
    }

    public ParquetCombinedInputSplit(InputSplit split) {
        this.combinedInputSplitList = Collections.singletonList((FileSplit) split);
    }

    public void add(InputSplit split) {
        this.combinedInputSplitList.add((FileSplit) split);
    }

    public List<FileSplit> getCombinedInputSplitList() {
        return combinedInputSplitList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(combinedInputSplitList.size());
        for (FileSplit inputSplit : combinedInputSplitList) {
            inputSplit.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int inputSplitSize = in.readInt();
        for (int i = 0; i < inputSplitSize; ++i) {
            ParquetInputSplit inputSplit = new ParquetInputSplit();
            inputSplit.readFields(in);
            combinedInputSplitList.add(inputSplit);
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return combinedInputSplitList.stream()
                .reduce(
                        0L,
                        (sum, inputSplit) -> sum + inputSplit.getLength(),
                        Long::sum
                );
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        Set<String> res = new TreeSet<>();
        for (FileSplit inputSplit : combinedInputSplitList) {
            res.addAll(Arrays.asList(inputSplit.getLocations()));
        }
        return res.toArray(new String[0]);
    }

    @Override
    public String toString() {
        return combinedInputSplitList.toString();
    }
}
