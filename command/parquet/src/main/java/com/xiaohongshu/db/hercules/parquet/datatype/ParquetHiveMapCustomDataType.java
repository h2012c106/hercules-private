package com.xiaohongshu.db.hercules.parquet.datatype;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import com.xiaohongshu.db.hercules.parquet.mr.input.GroupWithSchemaInfo;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputWrapperManager;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputWrapperManager;
import org.apache.parquet.example.data.Group;

import java.util.Map;
import java.util.function.Function;

/**
 * map格式形如:
 * optional group [列名] (MAP) {
 * repeated group map (MAP_KEY_VALUE) {
 * required binary key (STRING);
 * optional [自定类型] value;
 * }
 * }
 * 此处的MapWrapper的存储格式即为上述结构，而非普通map
 */
public class ParquetHiveMapCustomDataType extends CustomDataType<GroupWithSchemaInfo, Group, MapWrapper> {

    public static final ParquetHiveMapCustomDataType INSTANCE = new ParquetHiveMapCustomDataType();

    private static final String MAP_NAME = "map";
    private static final String KEY_NAME = "key";
    private static final String VAL_NAME = "value";

    protected ParquetHiveMapCustomDataType() {
        super("ParquetHiveMap", BaseDataType.MAP, MapWrapper.class, new Function<Object, BaseWrapper<?>>() {
            @Override
            public BaseWrapper<?> apply(Object o) {
                MapWrapper rawMap = (MapWrapper) o;
                ListWrapper kvList = (ListWrapper) rawMap.get(MAP_NAME);
                MapWrapper res = new MapWrapper(kvList.size());
                for (BaseWrapper<?> wrapper : kvList) {
                    MapWrapper kv = (MapWrapper) wrapper;
                    res.put(((StringWrapper) kv.get(KEY_NAME)).asString(), kv.get(VAL_NAME));
                }
                return res;
            }
        });
    }

    @Override
    protected BaseTypeWrapperGetter<MapWrapper, GroupWithSchemaInfo> createWrapperGetter(CustomDataType<GroupWithSchemaInfo, Group, MapWrapper> self) {
        return new BaseTypeWrapperGetter<MapWrapper, GroupWithSchemaInfo>() {
            @Override
            protected MapWrapper getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return (MapWrapper) ParquetInputWrapperManager.MAP_GETTER.get(row, rowName, columnName, columnSeq);
            }

            @Override
            protected DataType getType() {
                return self;
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter<MapWrapper, Group> createWrapperSetter(CustomDataType<GroupWithSchemaInfo, Group, MapWrapper> self) {
        return new BaseTypeWrapperSetter<MapWrapper, Group>() {
            @Override
            protected DataType getType() {
                return self;
            }

            @Override
            protected void setNonnullValue(MapWrapper value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                ParquetOutputWrapperManager.MAP_SETTER.set(value, row, rowName, columnName, columnSeq);
            }

            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }
        };
    }

    @Override
    protected MapWrapper innerWrite(MapWrapper wrapper) throws Exception {
        ListWrapper kvList = new ListWrapper(wrapper.entrySet().size());
        for (Map.Entry<String, BaseWrapper<?>> entry : wrapper.entrySet()) {
            MapWrapper kv = new MapWrapper(2);
            kv.put(KEY_NAME, StringWrapper.get(entry.getKey()));
            kv.put(VAL_NAME, entry.getValue());
            kvList.add(kv);
        }
        MapWrapper res = new MapWrapper(1);
        res.put(MAP_NAME, kvList);
        return res;
    }

    @Override
    public Class<?> getJavaClass() {
        return MapWrapper.class;
    }
}
