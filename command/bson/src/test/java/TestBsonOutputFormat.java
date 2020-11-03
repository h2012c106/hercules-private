import com.mongodb.hadoop.output.BSONFileRecordWriter;
import com.mongodb.hadoop.splitter.BSONSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.jupiter.api.Test;

public class TestBsonOutputFormat {

    @Test
    public void testGetPath(){

    }
}
