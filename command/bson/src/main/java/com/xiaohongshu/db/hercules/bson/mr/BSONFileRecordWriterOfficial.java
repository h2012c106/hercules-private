/*
 * Copyright 2011-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xiaohongshu.db.hercules.bson.mr;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;

import java.io.DataOutputStream;
import java.io.IOException;


public class BSONFileRecordWriterOfficial extends RecordWriter<String, BSONWritable> {

    private final BSONEncoder bsonEnc = new BasicBSONEncoder();
    private final DataOutputStream outFile;
    private final FSDataOutputStream splitsFile;
    private final long splitSize;
    private long bytesWritten = 0L;
    private long currentSplitLen = 0;
    private long currentSplitStart = 0;

    public BSONFileRecordWriterOfficial(final DataOutputStream outFile, final FSDataOutputStream splitsFile, final long splitSize) {
        this.outFile = outFile;
        this.splitsFile = splitsFile;
        this.splitSize = splitSize;
    }

    public void close(final TaskAttemptContext context) throws IOException {
        if (this.outFile != null) {
            this.outFile.close();
        }
        writeSplitData(0, true);
        if (this.splitsFile != null) {
            this.splitsFile.close();
        }
    }

    public void write(final String key, final BSONWritable value) throws IOException {
        byte[] outputByteBuf;
        outputByteBuf = bsonEnc.encode(value.getDoc());
        outFile.write(outputByteBuf, 0, outputByteBuf.length);
        bytesWritten += outputByteBuf.length;
        writeSplitData(outputByteBuf.length, false);
    }

    private void writeSplitData(final int docSize, final boolean force) throws IOException {
        //If no split file is being written, bail out now
        if (this.splitsFile == null) {
            return;
        }

        // hit the threshold of a split, write it to the metadata file
        if (force || currentSplitLen + docSize >= this.splitSize) {
            BSONObject splitObj = BasicDBObjectBuilder.start()
                    .add("s", currentSplitStart)
                    .add("l", currentSplitLen).get();
            byte[] encodedObj = this.bsonEnc.encode(splitObj);
            this.splitsFile.write(encodedObj, 0, encodedObj.length);

            //reset the split len and start
            this.currentSplitLen = 0;
            this.currentSplitStart = bytesWritten - docSize;
        } else {
            // Split hasn't hit threshold yet, just add size
            this.currentSplitLen += docSize;
        }
    }
}

