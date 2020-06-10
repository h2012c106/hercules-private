package com.xiaohongshu.db.hercules.parquet.schema;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.schema.*;

import static com.xiaohongshu.db.hercules.parquet.ParquetUtils.DEFAULT_PRECISION;
import static com.xiaohongshu.db.hercules.parquet.ParquetUtils.DEFAULT_SCALE;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

/**
 * 虽然在{@link ParquetDataTypeConverter}的注释中提到parquet有自己的存储格式，但是它只定义了底层存储的规范并不提供读写的范式（至少{@link org.apache.parquet.example.data.Group}没有）
 * 所以这里定义Hercules的策略
 */
public class ParquetHerculesDataTypeConverter extends ParquetDataTypeConverter {

    private static final Log LOG = LogFactory.getLog(ParquetHerculesDataTypeConverter.class);

    private static class ConverterHolder {
        private static ParquetHerculesDataTypeConverter INSTANCE = new ParquetHerculesDataTypeConverter();
    }

    public static ParquetHerculesDataTypeConverter getInstance() {
        return ConverterHolder.INSTANCE;
    }

    private ParquetHerculesDataTypeConverter() {
    }

    @Override
    public Types.Builder<?, ? extends Type> convertDataType(DataType dataType, Type.Repetition repetition) {
        if (!dataType.isCustom()) {
            switch ((BaseDataType) dataType) {
                case BYTE:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .as(LogicalTypeAnnotation.intType(8, true));
                case SHORT:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .as(LogicalTypeAnnotation.intType(16, true));
                case INTEGER:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition);
                case LONG:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition);
                case LONGLONG:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition);
                case BOOLEAN:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition);
                case FLOAT:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition);
                case DOUBLE:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition);
                case DECIMAL:
                    // 直接返回允许最大的，由于要借助hive逻辑，所以限制38/12
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                            .as(LogicalTypeAnnotation.decimalType(DEFAULT_SCALE, DEFAULT_PRECISION));
                case STRING:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                            .as(LogicalTypeAnnotation.stringType());
                case DATE:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .as(LogicalTypeAnnotation.dateType());
                case TIME:
                    // 用毫秒
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS));
                case DATETIME:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS));
                case BYTES:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition);
                case LIST:
                    // hercules的配置无法指定list的T，columnMap是没办法处理repeated内部的schema的
                    throw new RuntimeException(String.format("List type cannot be expressed in parquet schema. Please use '--%s'.", MESSAGE_TYPE));
                case MAP:
                    return Types.buildGroup(repetition);
                default:
                    throw new RuntimeException(String.format("The type [%s] cannot be expressed by parquet schema.", dataType.toString()));
            }
        } else {
            throw new RuntimeException(String.format("The type [%s] cannot be expressed by parquet schema.", dataType.toString()));
        }
    }

    @Override
    public DataType convertElementType(ParquetType standard) {
        Type type = standard.getType();
        boolean careRepeated = standard.careRepeated();

        if (careRepeated && type.isRepetition(Type.Repetition.REPEATED)) {
            return BaseDataType.LIST;
        }

        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        if (annotation != null) {
            OriginalType originalType = annotation.toOriginalType();
            switch (originalType) {
                case INT_8:
                    return BaseDataType.BYTE;
                case INT_16:
                    return BaseDataType.SHORT;
                case UTF8:
                    return BaseDataType.STRING;
                case DATE:
                    return BaseDataType.DATE;
                case TIME_MILLIS:
                    return BaseDataType.TIME;
                case TIMESTAMP_MILLIS:
                    return BaseDataType.DATETIME;
                case DECIMAL:
                    return BaseDataType.DECIMAL;
                default:
                    LOG.warn(String.format("The annotation [%s] is not supported at present, it will be treated as [%s] normally.",
                            getAnnotationName(annotation), getTypeName(type)));
            }
        }

        if (type.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName primitiveTypeName = ((PrimitiveType) type).getPrimitiveTypeName();
            switch (primitiveTypeName) {
                case INT32:
                    return BaseDataType.INTEGER;
                case INT64:
                    return BaseDataType.LONG;
                case INT96:
                    return BaseDataType.LONGLONG;
                case BOOLEAN:
                    return BaseDataType.BOOLEAN;
                case FLOAT:
                    return BaseDataType.FLOAT;
                case DOUBLE:
                    return BaseDataType.DOUBLE;
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return BaseDataType.BYTES;
                default:
                    throw new RuntimeException("Unknown parquet type: " + primitiveTypeName);
            }
        } else {
            return BaseDataType.MAP;
        }
    }
}
