package com.xiaohongshu.db.hercules.udf;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.udf.HerculesUDF;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.ErrorLoggerUtils;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import com.xiaohongshu.db.hercules.parquet.datatype.ParquetHiveListCustomDataType;
import com.xiaohongshu.db.hercules.parquet.datatype.ParquetHiveMapCustomDataType;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHiveDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 三种配置方式，直接给parquet schema/给options中提供schema的参数/给hive schema位置
 */
public class RowSchemaCheckUDF extends HerculesUDF {

    private static final String MESSAGE_TYPE_PROP = "hercules.schema.check.parquet.schema.udf";
    /**
     * 本参数结构形如'target-xxx'
     */
    private static final String MESSAGE_TYPE_OPTION_PROP = "hercules.schema.check.parquet.schema.option.udf";

    private static final String HIVE_META_CONNECTION_PROP = "hercules.schema.check.hivemeta.connection.udf";
    private static final String HIVE_META_USER_PROP = "hercules.schema.check.hivemeta.user.udf";
    private static final String HIVE_META_PASSWORD_PROP = "hercules.schema.check.hivemeta.password.udf";
    private static final String HIVE_META_DRIVER_PROP = "hercules.schema.check.hivemeta.driver.udf";
    private static final String HIVE_DATABASE_PROP = "hercules.schema.check.database.udf";
    private static final String HIVE_TABLE_PROP = "hercules.schema.check.table.udf";

    private static final String ALLOW_ERROR_TOLERANCE_PROP = "hercules.schema.check.error.tolerance.udf";

    private static final String ERROR_TAG = "Illegal Row Schema";

    private MapType type;
    private final ParquetDataTypeConverter converter = ParquetHiveDataTypeConverter.getInstance();

    private long seq = 0L;
    private long errorNum = 0L;
    private long errorTolerance;

    Type convertParquetType(org.apache.parquet.schema.Type parquetType) {
        // 如果是MessageType，无脑转GroupType，继续递归
        if (parquetType instanceof MessageType) {
            MessageType groupType = (MessageType) parquetType;
            MapType res = new MapType();
            for (org.apache.parquet.schema.Type child : groupType.getFields()) {
                res.put(child.getName(), convertParquetType(child));
            }
            return res;
        } else {
            DataType herculesType = converter.convertElementType(new ParquetType(parquetType, false));
            if (herculesType == ParquetHiveListCustomDataType.INSTANCE) {
                return new NormalType(herculesType, convertParquetType(parquetType.asGroupType().getType(0).asGroupType().getType(0)));
            } else if (herculesType == ParquetHiveMapCustomDataType.INSTANCE) {
                Type keyType = convertParquetType(parquetType.asGroupType().getType(0).asGroupType().getType("key"));
                Type valueType = convertParquetType(parquetType.asGroupType().getType(0).asGroupType().getType("value"));
                return new KeyValueType(herculesType, keyType, valueType);
            } else if (herculesType == BaseDataType.MAP) {
                MapType res = new MapType();
                for (org.apache.parquet.schema.Type child : parquetType.asGroupType().getFields()) {
                    res.put(child.getName(), convertParquetType(child));
                }
                return res;
            } else {
                return new NormalType(herculesType, null);
            }
        }
    }

    @Override
    public void initialize(Mapper.Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        errorTolerance = configuration.getLong(ALLOW_ERROR_TOLERANCE_PROP, Long.MAX_VALUE);

        MessageType messageType = null;
        if (configuration.get(MESSAGE_TYPE_PROP) != null) {
            String messageTypeStr = configuration.get(MESSAGE_TYPE_PROP);
            messageType = MessageTypeParser.parseMessageType(messageTypeStr);
        } else if (configuration.get(MESSAGE_TYPE_OPTION_PROP) != null) {
            String option = configuration.get(MESSAGE_TYPE_OPTION_PROP);
            if (option == null) {
                throw new RuntimeException(String.format("Please use '%s' or '%s' to specify message type.",
                        MESSAGE_TYPE_PROP, MESSAGE_TYPE_OPTION_PROP));
            }
            String optionTypeStr = option.split("-")[0];
            String optionName = option.substring(optionTypeStr.length() + 1);
            OptionsType optionsType = null;
            for (OptionsType type : OptionsType.values()) {
                if (StringUtils.equalsIgnoreCase(type.getParamPrefix(), optionTypeStr)) {
                    optionsType = type;
                    break;
                }
            }
            if (optionsType == null) {
                throw new RuntimeException("Unknown option type: " + optionTypeStr);
            }
            String messageTypeStr = HerculesContext.instance().getWrappingOptions().getGenericOptions(optionsType).getString(optionName, null);
            if (messageTypeStr == null) {
                throw new RuntimeException("Empty option: " + option);
            }
            messageType = MessageTypeParser.parseMessageType(messageTypeStr);
        } else if (configuration.get(HIVE_META_CONNECTION_PROP) != null) {
            Map<String, String> typeMap = SchemaUtils.fetchHiveTableSchema(
                    configuration.get(HIVE_META_CONNECTION_PROP),
                    configuration.get(HIVE_META_USER_PROP),
                    configuration.get(HIVE_META_PASSWORD_PROP),
                    configuration.get(HIVE_META_DRIVER_PROP),
                    configuration.get(HIVE_DATABASE_PROP),
                    configuration.get(HIVE_TABLE_PROP)
            );
            messageType = ParquetSchemaUtils.generateMessageTypeFromHiveMeta(typeMap);
        } else {
            throw new RuntimeException(String.format("Please specify the schema-fetch options: [%s] or [%s] or [%s].",
                    MESSAGE_TYPE_PROP,
                    MESSAGE_TYPE_OPTION_PROP,
                    StringUtils.join(Lists.newArrayList(HIVE_META_CONNECTION_PROP, HIVE_META_USER_PROP, HIVE_META_PASSWORD_PROP, HIVE_META_DRIVER_PROP, HIVE_DATABASE_PROP, HIVE_TABLE_PROP), ", "))
            );
        }
        if (messageType == null) {
            throw new NullPointerException("Empty message type.");
        }
        type = (MapType) convertParquetType(messageType);
    }

    boolean check(BaseWrapper<?> value, Type type) {
        if (value.getType().isCustom() && type.getDataType().isCustom()) {
            if (!StringUtils.equals(value.getType().getName(), type.getDataType().getName())) {
                return false;
            }
        } else {
            if (value.getType().getBaseDataType() != type.getDataType().getBaseDataType()) {
                return false;
            }
        }
        if (type instanceof MapType) {
            MapWrapper mapValue = (MapWrapper) value;
            MapType mapType = (MapType) type;
            for (Map.Entry<String, Type> entry : mapType.getChildren().entrySet()) {
                String columnName = entry.getKey();
                Type childType = entry.getValue();
                BaseWrapper<?> childValue = mapValue.get(columnName);
                // 允许上下游列对不齐
                if (childValue != null && !check(childValue, childType)) {
                    return false;
                }
            }
        } else if (type instanceof NormalType && type.getDataType() == ParquetHiveListCustomDataType.INSTANCE) {
            for (BaseWrapper<?> item : (Iterable<BaseWrapper<?>>) value) {
                if (!check(item, ((NormalType) type).getChild())) {
                    return false;
                }
            }
        } else if (type instanceof KeyValueType) {
            KeyValueType keyValueType = (KeyValueType) type;
            // 由于现在MapWrapper的key是String而不是BaseWrapper
            if (keyValueType.getKeyType().getDataType() != BaseDataType.STRING) {
                throw new RuntimeException("Don't support non-string type map key: " + keyValueType.getKeyType().getDataType());
            }
            for (BaseWrapper<?> valueItem : ((Map<?, BaseWrapper<?>>) value).values()) {
                if (!check(valueItem, keyValueType.getValueType())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public HerculesWritable evaluate(HerculesWritable row) throws IOException, InterruptedException {
        ++seq;
        if (!check(row.getRow(), type)) {
            ErrorLoggerUtils.add(ERROR_TAG, row, OverflowUtils.numberToInteger(seq));
            if (++errorNum >= errorTolerance) {
                throw new RuntimeException(String.format("More than <%d> row(s) are checked as illegal: %d.", errorTolerance, errorNum));
            }
            return null;
        } else {
            return row;
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        ErrorLoggerUtils.print(ERROR_TAG);
    }

    static class Type {
        private DataType dataType;

        public Type(DataType dataType) {
            this.dataType = dataType;
        }

        public DataType getDataType() {
            return dataType;
        }
    }

    static class NormalType extends Type {
        private Type child;

        public NormalType(DataType dataType, Type child) {
            super(dataType);
            this.child = child;
        }

        public Type getChild() {
            return child;
        }
    }

    static class MapType extends Type {
        private Map<String, Type> children = new HashMap<>();

        public MapType() {
            super(BaseDataType.MAP);
        }

        public void put(String column, Type type) {
            children.put(column, type);
        }

        public Map<String, Type> getChildren() {
            return children;
        }
    }

    static class KeyValueType extends Type {
        private Type keyType;
        private Type valueType;

        public KeyValueType(DataType dataType, Type keyType, Type valueType) {
            super(dataType);
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public Type getKeyType() {
            return keyType;
        }

        public Type getValueType() {
            return valueType;
        }
    }
}
