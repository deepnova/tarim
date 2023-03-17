package org.deepexi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.shaded.org.apache.avro.LogicalType;
import org.apache.iceberg.shaded.org.apache.avro.Schema;
import org.apache.iceberg.shaded.org.apache.avro.SchemaParseException;
import org.apache.iceberg.shaded.org.apache.avro.LogicalTypes;
import java.util.List;


public class TarimTypeToFlinkType {

    TarimTypeToFlinkType(){

    }

    //reference AvroSchemaConverter.convertToDataType
    public static DataType convertToDataType(String avroSchemaString) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");

        Schema schema;
        try {
            schema = (new Schema.Parser()).parse(avroSchemaString);
        } catch (SchemaParseException exception) {
            throw new IllegalArgumentException("Could not parse Avro schema string." + exception);
        }

        return convertToDataType(schema);
    }

    public static DataType convertToDataType(Schema schema) {
        LogicalTypes.Decimal decimalType;
        switch (schema.getType()) {
            case RECORD:
                List<Schema.Field> schemaFields = schema.getFields();
                DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];

                for(int i = 0; i < schemaFields.size(); ++i) {
                    Schema.Field field = (Schema.Field)schemaFields.get(i);
                    fields[i] = DataTypes.FIELD(field.name(), convertToDataType(field.schema()));
                }

                return (DataType)DataTypes.ROW(fields).notNull();
            case ENUM:
                return (DataType)DataTypes.STRING().notNull();
            case ARRAY:
                return (DataType)DataTypes.ARRAY(convertToDataType(schema.getElementType())).notNull();
            case MAP:
                return (DataType)DataTypes.MAP((DataType)DataTypes.STRING().notNull(), convertToDataType(schema.getValueType())).notNull();
            case UNION:
                Schema actualSchema;
                boolean nullable;
                if (schema.getTypes().size() == 2 && (schema.getTypes().get(0)).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(1);
                    nullable = true;
                } else if (schema.getTypes().size() == 2 && (schema.getTypes().get(1)).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(0);
                    nullable = true;
                } else {
                    if (schema.getTypes().size() != 1) {
                        return new AtomicDataType(new TypeInformationRawType(false, Types.GENERIC(Object.class)));
                    }

                    actualSchema = schema.getTypes().get(0);
                    nullable = false;
                }

                DataType converted = convertToDataType(actualSchema);
                return nullable ? (DataType)converted.nullable() : converted;
            case FIXED:
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    decimalType = (LogicalTypes.Decimal)schema.getLogicalType();
                    return (DataType)DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale()).notNull();
                }

                return (DataType)DataTypes.VARBINARY(schema.getFixedSize()).notNull();
            case STRING:
                return (DataType)DataTypes.STRING().notNull();
            case BYTES:
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    decimalType = (LogicalTypes.Decimal)schema.getLogicalType();
                    return (DataType)DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale()).notNull();
                }

                return (DataType)DataTypes.BYTES().notNull();
            case INT:
                LogicalType logicalType = schema.getLogicalType();
                if (logicalType == LogicalTypes.date()) {
                    return (DataType)DataTypes.DATE().notNull();
                } else {
                    if (logicalType == LogicalTypes.timeMillis()) {
                        return (DataType)DataTypes.TIME(3).notNull();
                    }

                    return (DataType)DataTypes.INT().notNull();
                }
            case LONG:
                if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
                    return (DataType)DataTypes.TIMESTAMP(3).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                    return (DataType)DataTypes.TIMESTAMP(6).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
                    return (DataType)DataTypes.TIME(3).notNull();
                } else {
                    if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
                        return (DataType)DataTypes.TIME(6).notNull();
                    }

                    return (DataType)DataTypes.BIGINT().notNull();
                }
            case FLOAT:
                return (DataType)DataTypes.FLOAT().notNull();
            case DOUBLE:
                return (DataType)DataTypes.DOUBLE().notNull();
            case BOOLEAN:
                return (DataType)DataTypes.BOOLEAN().notNull();
            case NULL:
                return DataTypes.NULL();
            default:
                throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
        }
    }
}
