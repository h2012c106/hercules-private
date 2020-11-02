package com.xiaohongshu.db.hercules.parquet.datatype;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.parquet.mr.input.GroupWithSchemaInfo;
import com.xiaohongshu.db.hercules.parquet.mr.input.ParquetInputWrapperManager;
import com.xiaohongshu.db.hercules.parquet.mr.output.ParquetOutputWrapperManager;
import org.apache.parquet.example.data.Group;

import java.util.function.Function;

/**
 * list格式形如:
 * optional group [列名] (LIST) {
 * repeated group bag {
 * optional [自定类型] array_element;
 * }
 * }
 * 此处的MapWrapper的存储格式即为上述结构，而非普通map
 */
public class ParquetHiveListCustomDataType extends CustomDataType<GroupWithSchemaInfo, Group, MapWrapper> {
    public static final ParquetHiveListCustomDataType INSTANCE = new ParquetHiveListCustomDataType();

    private static final String LIST_NAME = "bag";
    private static final String ITEM_NAME = "array_element";

    protected ParquetHiveListCustomDataType() {
        super("ParquetHiveList", BaseDataType.LIST, MapWrapper.class, new Function<Object, BaseWrapper<?>>() {
            @Override
            public BaseWrapper<?> apply(Object o) {
                MapWrapper rawMap = (MapWrapper) o;
                ListWrapper bag = (ListWrapper) rawMap.get(LIST_NAME);
                if (bag == null) {
                    throw new RuntimeException("Illegal hive list format for map column: " + o);
                }
                ListWrapper res = new ListWrapper(bag.size());
                for (BaseWrapper<?> item : bag) {
                    MapWrapper mapItem = (MapWrapper) item;
                    BaseWrapper<?> properItem = mapItem.get(ITEM_NAME);
                    if (properItem == null) {
                        throw new RuntimeException("Illegal hive list format for map column: " + o);
                    }
                    res.add(properItem);
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
    protected MapWrapper innerWrite(ListWrapper wrapper) throws Exception {
        ListWrapper bag = new ListWrapper(wrapper.size());
        for (BaseWrapper<?> item : wrapper) {
            MapWrapper mapItem = new MapWrapper(1);
            mapItem.put(ITEM_NAME, item);
            bag.add(mapItem);
        }
        MapWrapper res = new MapWrapper(1);
        res.put(LIST_NAME, bag);
        return res;
    }

    @Override
    public Class<?> getJavaClass() {
        return MapWrapper.class;
    }
}
