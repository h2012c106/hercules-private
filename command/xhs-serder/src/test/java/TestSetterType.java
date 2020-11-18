import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class TestSetterType {

    private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .build();

    @Test
    public void TestMongoDocumentToJson() {
        Document document = new Document();
        document.put("bigDecimal", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(20)));
        System.out.println(document.toJson(JSON_WRITER_SETTINGS));
        //9223372036854775827
    }
}
