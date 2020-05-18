package com.xiaohongshu.db.hercules.parquet.schema;

import com.xiaohongshu.db.hercules.core.serialize.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.schema.*;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.MESSAGE_TYPE;

/**
 * Sqoop策略的转换器
 * decimal -> string
 * date/time/datetime -> long
 */
public class ParquetSqoopDataTypeConverter extends ParquetDataTypeConverter {

    private static final Log LOG = LogFactory.getLog(ParquetSqoopDataTypeConverter.class);

    private static class ConverterHolder {
        private static ParquetSqoopDataTypeConverter INSTANCE = new ParquetSqoopDataTypeConverter();
    }

    public static ParquetSqoopDataTypeConverter getInstance() {
        return ConverterHolder.INSTANCE;
    }

    private ParquetSqoopDataTypeConverter() {
    }

    /**
     * 若需要某一列为{@link DataType#DECIMAL}、{@link DataType#DATE}、{@link DataType#TIME}、{@link DataType#DATETIME}、
     * {@link DataType#BYTE}、{@link DataType#SHORT}，需要用columnMap指定，单凭parquet schema无法认出来
     */
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
            if (originalType == OriginalType.UTF8) {
                return DataType.STRING;
            } else {
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

    @Override
    protected Types.Builder<?, ? extends Type> convertDataType(DataType dataType, Type.Repetition repetition) {
        switch (dataType) {
            case BYTE:
            case SHORT:
            case INTEGER:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition);
            case LONG:
            case TIME:
            case DATE:
            case DATETIME:
                // sqoop从数据库读出date/time/timestamp会转成avro的long，且用getTime()方法转，相关逻辑在AvroUtil.toAvro()方法
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
            case STRING:
                // sqoop从数据库读出decimal还是会转成avro的string，相关逻辑在AvroUtil.toAvro()方法
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(LogicalTypeAnnotation.stringType());
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
}
