package com.xiaohongshu.db.hercules.bson.mr;

import com.mongodb.BasicDBObject;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xiaohongshu.db.hercules.bson.option.BsonOptionsConf;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.bson.Document;

import java.io.DataOutputStream;
import java.io.IOException;

public class BsonOutputFormat extends HerculesOutputFormat<Document> {

    private BSONFileOutputFormatCus delegate;

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    @Override
    protected HerculesRecordWriter<Document> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        delegate = new BSONFileOutputFormatCus(options.getString(BsonOptionsConf.DIR, ""));
        return new BsonRecordWriter(context, delegate.getRecordWriter(context));
    }

    @Override
    protected WrapperSetterFactory<Document> createWrapperSetterFactory() {
        return new BsonWrapperSetterManager();
    }

    static class BSONFileOutputFormatCus extends FileOutputFormat<String, BSONWritable>{

        private final String outPathParent;

        BSONFileOutputFormatCus(String outPathParent) {
            this.outPathParent = outPathParent;
        }

        @Override
        public RecordWriter<String, BSONWritable> getRecordWriter(TaskAttemptContext context) throws IOException {

            boolean isCompressed = getCompressOutput(context);
            CompressionCodec codec = null;
            String extension = "";
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass =
                        getOutputCompressorClass(context, GzipCodec.class);
                codec = ReflectionUtils.newInstance(codecClass, context.getConfiguration());
                extension = codec.getDefaultExtension();
            }

            Path path = getDefaultWorkFile(context, ".bson" + extension);
            Path outPath = new Path(outPathParent, path.getName());

            FileSystem fs = outPath.getFileSystem(context.getConfiguration());
            FSDataOutputStream outFile = fs.create(outPath);
            // 写splits的逻辑，暂时也保留下来
            FSDataOutputStream splitFile = null;
            if (MongoConfigUtil.getBSONOutputBuildSplits(context.getConfiguration())) {
                Path splitPath = new Path(outPath.getParent(), "." + outPath.getName() + ".splits");
                splitFile = fs.create(splitPath);
            }
            long splitSize = BSONSplitter.getSplitSize(context.getConfiguration(), null);
            if (isCompressed) {
                DataOutputStream out = new DataOutputStream(codec.createOutputStream(outFile));
                return new BSONFileRecordWriterOfficial(out, splitFile, splitSize);
            }
            return new BSONFileRecordWriterOfficial(outFile, splitFile, splitSize);
        }
    }
}

class BsonRecordWriter extends HerculesRecordWriter<Document> {

    private RecordWriter<String, BSONWritable> delegate;

    public BsonRecordWriter(TaskAttemptContext context, RecordWriter<String, BSONWritable> bsonFileRecordWriter) {
        super(context);
        delegate = bsonFileRecordWriter;
    }

    @Override
    protected void innerWrite(HerculesWritable in) throws IOException, InterruptedException {
        Document document;
        try {
            document = wrapperSetterFactory.writeMapWrapper(in.getRow(), new Document(), null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        BasicDBObject doc = new BasicDBObject(document);
        BSONWritable value = new BSONWritable();
        value.setDoc(doc);
        delegate.write(null, value);
    }

    @Override
    protected WritableUtils.FilterUnexistOption getColumnUnexistOption() {
        return null;
    }

    @Override
    protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.close(context);
    }
}