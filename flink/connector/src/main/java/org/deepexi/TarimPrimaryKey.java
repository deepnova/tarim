package org.deepexi;

import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.StructLike;

import java.io.Serializable;
import org.apache.iceberg.*;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class TarimPrimaryKey implements StructLike, Serializable {
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    private final int size;
    private final Object[] primaryKeyTuple;
    private final Accessor<StructLike>[] accessors;
    private List<String> primaryKeys;
    private List<Type.TypeID> types;
    private static String KEY_SEPARATOR = "_";

    public TarimPrimaryKey(List<String> primaryKeys, List<Type.TypeID> types, List<Integer> ids, Schema schema){
        this.size = primaryKeys.size();
        this.primaryKeyTuple = new Object[this.size];
        this.accessors = (Accessor[]) Array.newInstance(Accessor.class, this.size);
        this.primaryKeys = primaryKeys;
        this.types = types;
        for(int i = 0; i < this.size; ++i) {
            Accessor<StructLike> accessor = schema.accessorForField(ids.get(i));
            this.accessors[i] = accessor;
        }

    }

    public Object[] primaryData(StructLike row){

        Object object[] = new Object[this.size];
        for (int i = 0; i < this.size; i++){
            object[i] = this.accessors[i].get(row);
        }
        return object;

    }
    public List<String> getPrimaryKeys(){
        return this.primaryKeys;
    }

    public List<Type.TypeID> getTypes(){
        return this.types;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public <T> T get(int i, Class<T> aClass) {
        return aClass.cast(this.primaryKeyTuple[i]);
    }

    @Override
    public <T> void set(int i, T value) {
        this.primaryKeyTuple[i] = value;
    }

    public String codecPrimaryValue(List<Object> primaryKeyValues){
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < primaryKeyValues.size();i++){
            if (i != 0){
                buf.append(KEY_SEPARATOR);
            }

            Type.TypeID id = types.get(i);
            switch (id){
                case INTEGER:
                    buf.append(keyIntBase16Codec(Integer.parseInt(primaryKeyValues.get(i).toString())));
                    //buf.append(keyLongBase16Codec(Long.parseLong(primaryKeyValues.get(i))));
                    //buf.append(keyLongBase16Codec(Long.parseLong(primaryKeyValues.get(i).toString())));
                    break;
                case LONG:
                    buf.append(keyLongBase16Codec(Long.parseLong(primaryKeyValues.get(i).toString())));
                    break;
                case STRING:
                    buf.append(primaryKeyValues.get(i).toString());
                    break;
                case TIME:
                case TIMESTAMP:
                case DATE:
                    //todo
                default:
                    throw new RuntimeException("un support primary type!");

            }

        }
        return buf.toString();
    }

    public String keyLongBase16Codec(long value) {
        value = value ^ 0x8000000000000000L;

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        byte[] bytes = buffer.array();
        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }

        return new String(chars);
    }

    public String keyIntBase16Codec(int value) {
        value = value ^ 0x80000000;

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        byte[] bytes = buffer.array();

        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }

        return new String(chars);
    }

    public int comparePrimary(Object[] mainPkMin, Object[] deltaPkMin, Schema schema, List<Integer> idsList){

        for (Integer id : idsList){
            Type type = schema.findType(id);
            Type.TypeID typeId = type.typeId();

            Object mainObject = mainPkMin[id - 1];
            Object deltaObject = deltaPkMin[id - 1];

            switch(typeId){
                case INTEGER:
                    Integer mainKey = (Integer)mainObject;
                    Integer deltaKey = (Integer)deltaObject;
                    if (mainKey > deltaKey){
                        return 1;
                    }else if (mainKey < deltaKey){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case LONG:
                    Long mainLongKey = (Long)mainObject;
                    Long deltaLongKey = (Long)deltaObject;
                    if (mainLongKey > deltaLongKey){
                        return 1;
                    }else if (mainLongKey < deltaLongKey){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case STRING:
                    String mainStrKey = (String)mainObject;
                    String deltaStrKey = (String)deltaObject;

                    int result = mainStrKey.compareTo(deltaStrKey);
                    if (result > 0){
                        return 1;
                    }else if (result < 0){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case DATE:
                    int mainData = (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate)mainObject);
                    int deltaData = (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate)deltaObject);

                    if (mainData > deltaData){
                        return 1;
                    }else if (mainData < deltaData){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                case TIME:
                    LocalTime mainLocalTime = (LocalTime)mainObject;
                    long mainTimeData = TimeUnit.NANOSECONDS.toMillis(mainLocalTime.toNanoOfDay());
                    LocalTime deltaLocalTime = (LocalTime)deltaObject;
                    long deltaTimeData = TimeUnit.NANOSECONDS.toMillis(deltaLocalTime.toNanoOfDay());

                    if (mainTimeData > deltaTimeData){
                        return 1;
                    }else if (mainTimeData < deltaTimeData){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }


                case TIMESTAMP:
                    TimestampData mainTimeStamp;
                    TimestampData deltaTimeStamp;
                    if (((Types.TimestampType)type).shouldAdjustToUTC()) {
                        mainTimeStamp = TimestampData.fromInstant(((OffsetDateTime) mainObject).toInstant());
                        deltaTimeStamp = TimestampData.fromInstant(((OffsetDateTime) deltaObject).toInstant());

                    } else {
                        mainTimeStamp = TimestampData.fromLocalDateTime((LocalDateTime) mainObject);
                        deltaTimeStamp = TimestampData.fromLocalDateTime((LocalDateTime) deltaObject);
                    }

                    int rel = mainTimeStamp.compareTo(deltaTimeStamp);
                    if (rel > 0){
                        return 1;
                    }else if (rel < 0){
                        return -1;
                    }else{
                        //next key
                        continue;
                    }

                default:
                    throw new RuntimeException("un support Type !!");
            }
        }

        return 0;
    }
}
