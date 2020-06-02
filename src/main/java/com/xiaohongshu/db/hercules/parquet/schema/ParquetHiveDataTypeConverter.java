package com.xiaohongshu.db.hercules.parquet.schema;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.schema.*;

import java.util.Set;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

public class ParquetHiveDataTypeConverter extends ParquetDataTypeConverter {

    private static final Log LOG = LogFactory.getLog(ParquetHiveDataTypeConverter.class);

    private static class ConverterHolder {
        private static ParquetHiveDataTypeConverter INSTANCE = new ParquetHiveDataTypeConverter();
    }

    public static ParquetHiveDataTypeConverter getInstance() {
        return ConverterHolder.INSTANCE;
    }

    private ParquetHiveDataTypeConverter() {
    }

    @Override
    public Types.Builder<?, ? extends Type> convertDataType(DataType dataType, Type.Repetition repetition) {
        switch (dataType) {
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
            // case LONGLONG:
            // sqoop与hive根本不会遇到这个逻辑类型
            case BOOLEAN:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition);
            case FLOAT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition);
            case DOUBLE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition);
            case DECIMAL:
                throw new RuntimeException(String.format("Cannot convert decimal from data type to parquet schema, " +
                        "the precision and scale cannot be supplied by hercules. " +
                        "If you insist, please use '--%s' to specify.", MESSAGE_TYPE));
            case STRING:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(LogicalTypeAnnotation.stringType());
            case DATE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(LogicalTypeAnnotation.dateType());
            case DATETIME:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition);
            case BYTES:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition);
            case LIST:
                // hercules的配置无法指定list的T，columnMap是没办法处理repeated内部的schema的
                throw new RuntimeException(String.format("List type cannot be expressed in parquet schema. Please use '--%s'.", MESSAGE_TYPE));
            case MAP:
                return Types.buildGroup(repetition);
            default:
                throw new RuntimeException(String.format("The type [%s] cannot be expressed by parquet schema.", dataType));
        }
    }

    @Override
    public Set<DataType> getSupportedDataTypeSet() {
        return Sets.newHashSet(
                DataType.BYTE,
                DataType.SHORT,
                DataType.INTEGER,
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.DECIMAL,
                DataType.BOOLEAN,
                DataType.DATE,
                DataType.DATETIME,
                DataType.STRING,
                DataType.BYTES,
                DataType.LIST,
                DataType.MAP
        );
    }

    @Override
    public DataType convertElementType(ParquetType standard) {
        Type type = standard.getType();
        boolean careRepeated = standard.careRepeated();

        if (careRepeated && type.isRepetition(Type.Repetition.REPEATED)) {
            return DataType.LIST;
        }

        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        if (annotation != null) {
            OriginalType originalType = annotation.toOriginalType();
            switch (originalType) {
                case INT_8:
                    return DataType.BYTE;
                case INT_16:
                    return DataType.SHORT;
                case UTF8:
                    return DataType.STRING;
                case DATE:
                    return DataType.DATE;
                case DECIMAL:
                    return DataType.DECIMAL;
                default:
                    LOG.warn(String.format("The annotation [%s] is not supported at present, it will be treated as [%s] normally.",
                            getAnnotationName(annotation), getTypeName(type)));
            }
        }

        if (type.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName primitiveTypeName = ((PrimitiveType) type).getPrimitiveTypeName();
            switch (primitiveTypeName) {
                case INT32:
                    return DataType.INTEGER;
                case INT64:
                    return DataType.LONG;
                case INT96:
                    // 没有Longlong类型，int96一定是timestamp
                    return DataType.DATETIME;
                case BOOLEAN:
                    return DataType.BOOLEAN;
                case FLOAT:
                    return DataType.FLOAT;
                case DOUBLE:
                    return DataType.DOUBLE;
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return DataType.BYTES;
                default:
                    throw new RuntimeException("Unknown parquet type: " + primitiveTypeName);
            }
        } else {
            return DataType.MAP;
        }
    }
}
