package com.xiaohongshu.db.hercules.udf;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import com.xiaohongshu.db.hercules.parquet.datatype.ParquetHiveListCustomDataType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RowSchemaCheckTest {

    private RowSchemaCheckUDF udf = new RowSchemaCheckUDF();

    private RowSchemaCheckUDF.Type type = udf.convertParquetType(MessageTypeParser.parseMessageType("message hive_schema {\n" +
            "  optional group variants (LIST) {\n" +
            "    repeated group bag {\n" +
            "      optional group array_element {\n" +
            "        optional binary id (STRING);\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "  optional binary id (UTF8);\n" +
            "}"));

    @Test
    public void testConvertType() {
        RowSchemaCheckUDF.Type type = this.type;
        Assertions.assertTrue(type instanceof RowSchemaCheckUDF.MapType);
        type = ((RowSchemaCheckUDF.MapType) type).getChildren().get("variants");
        Assertions.assertEquals(type.getDataType(), ParquetHiveListCustomDataType.INSTANCE);
        RowSchemaCheckUDF.NormalType normalType = (RowSchemaCheckUDF.NormalType) type;
        Assertions.assertTrue(normalType.getChild() instanceof RowSchemaCheckUDF.MapType);
        Assertions.assertNotNull(((RowSchemaCheckUDF.MapType) normalType.getChild()).getChildren().get("id"));
        Assertions.assertEquals(((RowSchemaCheckUDF.MapType) normalType.getChild()).getChildren().get("id").getDataType(), BaseDataType.STRING);
    }

    @Test
    public void testCheckSuccess() {
        MapWrapper subMap = new MapWrapper();
        subMap.put("id", StringWrapper.get("TEST"));
        ListWrapper list = new ListWrapper();
        list.add(subMap);
        MapWrapper map = new MapWrapper();
        map.put("variants", list);
        Assertions.assertTrue(udf.check(map, type));

        map.put("id",StringWrapper.get("TEST"));
        Assertions.assertTrue(udf.check(map, type));
    }

    @Test
    public void testCheckFail() {
        MapWrapper subMap = new MapWrapper();
        subMap.put("id", IntegerWrapper.get(10L));
        ListWrapper list = new ListWrapper();
        list.add(subMap);
        MapWrapper map = new MapWrapper();
        map.put("variants", list);

        Assertions.assertFalse(udf.check(map, type));
    }

}
