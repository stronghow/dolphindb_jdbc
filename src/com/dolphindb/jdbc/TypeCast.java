package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Utils;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.List;

public class TypeCast {

    private static final String PACKAGE_NAME = "com.xxdb.data.";

    private static final String VECTOR = "Vector";

    // dateTime
    public static final String BASIC_MONTH = PACKAGE_NAME + "BasicMonth";
    public static final String BASIC_DATE = PACKAGE_NAME + "BasicDate";
    public static final String BASIC_TIME = PACKAGE_NAME + "BasicTime";
    public static final String BASIC_MINUTE = PACKAGE_NAME + "BasicMinute";
    public static final String BASIC_SECOND= PACKAGE_NAME + "BasicSecond";
    public static final String BASIC_NANOTIME = PACKAGE_NAME + "BasicNanoTime";
    public static final String BASIC_TIMESTAMP = PACKAGE_NAME + "BasicTimestamp";
    public static final String BASIC_DATETIME = PACKAGE_NAME + "BasicDateTime";
    public static final String BASIC_NANOTIMESTAMP = PACKAGE_NAME + "BasicNanoTimestamp";

    public static final String BASIC_MONTH_VECTOR = BASIC_MONTH + VECTOR;
    public static final String BASIC_DATE_VECTOR = BASIC_DATE + VECTOR;
    public static final String BASIC_TIME_VECTOR = BASIC_TIME + VECTOR;
    public static final String BASIC_MINUTE_VECTOR = BASIC_MINUTE + VECTOR;
    public static final String BASIC_SECOND_VECTOR = BASIC_SECOND + VECTOR;
    public static final String BASIC_NANOTIME_VECTOR = BASIC_NANOTIME + VECTOR;
    public static final String BASIC_TIMESTAMP_VECTOR = BASIC_TIMESTAMP + VECTOR;
    public static final String BASIC_DATETIME_VECTOR = BASIC_DATETIME + VECTOR;
    public static final String BASIC_NANOTIMESTAMP_VECTOR = BASIC_NANOTIMESTAMP + VECTOR;

    public static final String DATE = "java.sql.Date";
    public static final String TIME = "java.sql.Time";
    public static final String TIMESTAMP = "java.sql.Timestamp";
    public static final String LOCAL_DATE = "java.time.LocalDate";
    public static final String LOCAL_TIME = "java.time.LocalTime";
    public static final String LOCAL_DATETIME = "java.time.LocalDateTime";
    public static final String YEAR_MONTH = "java.time.YearMonth";

    //basicType
    public static final String BASIC_BOOLEAN = PACKAGE_NAME + "BasicBoolean";
    public static final String BASIC_BYTE = PACKAGE_NAME + "BasicByte";
    public static final String BASIC_SHORT = PACKAGE_NAME + "BasicShort";
    public static final String BASIC_INT = PACKAGE_NAME + "BasicInt";
    public static final String BASIC_LONG= PACKAGE_NAME + "BasicLong";
    public static final String BASIC_FLOAT = PACKAGE_NAME + "BasicFloat";
    public static final String BASIC_DOUBLE = PACKAGE_NAME + "BasicDouble";
    public static final String BASIC_STRING = PACKAGE_NAME + "BasicString";

    public static final String BASIC_BOOLEAN_VECTOR = BASIC_BOOLEAN + VECTOR;
    public static final String BASIC_BYTE_VECTOR = BASIC_BYTE + VECTOR;
    public static final String BASIC_SHORT_VECTOR = BASIC_SHORT + VECTOR;
    public static final String BASIC_INT_VECTOR = BASIC_INT + VECTOR;
    public static final String BASIC_LONG_VECTOR= BASIC_LONG + VECTOR;
    public static final String BASIC_FLOAT_VECTOR = BASIC_FLOAT + VECTOR;
    public static final String BASIC_DOUBLE_VECTOR = BASIC_DOUBLE + VECTOR;
    public static final String BASIC_STRING_VECTOR = BASIC_STRING + VECTOR;

    public static final String BOOLEAN = "java.lang.Boolean";
    public static final String BYTE = "java.lang.Byte";
    public static final String CHAR = "java.lang.Character";
    public static final String SHORT = "java.lang.Short";
    public static final String INT = "java.lang.Integer";
    public static final String LONG = "java.lang.Long";
    public static final String FLOAT = "java.lang.Float";
    public static final String DOUBLE = "java.lang.Double";
    public static final String STRING = "java.lang.String";

    public static Temporal getTemporal(Object value) throws Exception{
        switch (value.getClass().getName()){
            case BASIC_MONTH:
                return ((BasicMonth) value).getTemporal();
            case BASIC_DATE:
                return ((BasicDate) value).getTemporal();
            case BASIC_TIME:
                return ((BasicTime) value).getTemporal();
            case BASIC_MINUTE:
                return ((BasicMinute) value).getTemporal();
            case BASIC_SECOND:
                return ((BasicSecond) value).getTemporal();
            case BASIC_NANOTIME:
                return ((BasicNanoTime) value).getTemporal();
            case BASIC_TIMESTAMP:
                return ((BasicTimestamp) value).getTemporal();
            case BASIC_DATETIME:
                return ((BasicDateTime) value).getTemporal();
            case BASIC_NANOTIMESTAMP:
                return ((BasicNanoTimestamp) value).getTemporal();
            case DATE:
                return new BasicDate(((Date) value).toLocalDate()).getTemporal();
            case TIME:
                return new BasicNanoTime(((Time) value).toLocalTime()).getTemporal();
            case TIMESTAMP:
                return new BasicNanoTimestamp(((Timestamp) value).toLocalDateTime()).getTemporal();
            case LOCAL_DATE:
                return new BasicDate(((LocalDate) value)).getTemporal();
            case LOCAL_TIME:
                return new BasicNanoTime((LocalTime) value).getTemporal();
            case LOCAL_DATETIME:
                return new BasicNanoTimestamp((LocalDateTime) value).getTemporal();
            case YEAR_MONTH:
                return new BasicMonth((YearMonth) value).getTemporal();
            default:
                return null;
        }
    }

    public static long getDateTimeValue(Object value){
        switch (value.getClass().getName()){
            case BASIC_MONTH:
                return ((BasicMonth) value).getInt();
            case BASIC_DATE:
                return ((BasicDate) value).getInt();
            case BASIC_TIME:
                return ((BasicTime) value).getInt();
            case BASIC_MINUTE:
                return ((BasicMinute) value).getInt();
            case BASIC_SECOND:
                return ((BasicSecond) value).getInt();
            case BASIC_NANOTIME:
                return ((BasicNanoTime) value).getLong();
            case BASIC_TIMESTAMP:
                return ((BasicTimestamp) value).getLong();
            case BASIC_DATETIME:
                return ((BasicDateTime) value).getInt();
            case BASIC_NANOTIMESTAMP:
                return ((BasicNanoTimestamp) value).getLong();
            case DATE:
                return Utils.countDays(((Date) value).toLocalDate());
            case TIME:
                return Utils.countSeconds(((Time) value).toLocalTime());
            case TIMESTAMP:
                return Utils.countMilliseconds(((Time) value).toLocalTime());
            case LOCAL_DATE:
                return Utils.countDays((LocalDate) value);
            case LOCAL_TIME:
                return Utils.countSeconds((LocalTime) value);
            case LOCAL_DATETIME:
                return Utils.countMilliseconds(((LocalDateTime) value));
            case YEAR_MONTH:
                return Utils.countMonths(((YearMonth) value));
            default:
                return -1;
        }
    }

    public static Entity DataTime_temporl2db(Temporal temporal, String srcClassName, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        switch (targetEntityClassName){
            case BASIC_MONTH:
                return new BasicMonth(YearMonth.from(temporal));
            case BASIC_DATE:
                return new BasicDate(LocalDate.from(temporal));
            case BASIC_TIME:
                return new BasicTime(LocalTime.from(temporal));
            case BASIC_MINUTE:
                return new BasicMinute(LocalTime.from(temporal));
            case BASIC_SECOND:
                return new BasicSecond(LocalTime.from(temporal));
            case BASIC_NANOTIME:
                return new BasicNanoTime(LocalTime.from(temporal));
            case BASIC_TIMESTAMP:
                return new BasicTimestamp(LocalDateTime.from(temporal));
            case BASIC_DATETIME:
                return new BasicDateTime(LocalDateTime.from(temporal));
            case BASIC_NANOTIMESTAMP:
                return new BasicNanoTimestamp(LocalDateTime.from(temporal));
            default:
                throw new SQLException(srcClassName + " can not cast " + targetEntityClassName);
        }
    }

    public static Entity DataTime_long2db(long value, String srcClassName, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        switch (targetEntityClassName){
            case BASIC_MONTH:
                return new BasicMonth(Utils.parseMonth((int)value));
            case BASIC_DATE:
                return new BasicDate(Utils.parseDate((int)value));
            case BASIC_TIME:
                return new BasicTime(Utils.parseTime((int)value));
            case BASIC_MINUTE:
                return new BasicMinute(Utils.parseMinute((int)value));
            case BASIC_SECOND:
                return new BasicSecond(Utils.parseSecond((int)value));
            case BASIC_NANOTIME:
                return new BasicNanoTime(Utils.parseNanoTime(value));
            case BASIC_TIMESTAMP:
                return new BasicTimestamp(Utils.parseNanoTimestamp(value));
            case BASIC_DATETIME:
                return new BasicDateTime(Utils.parseDateTime((int)value));
            case BASIC_NANOTIMESTAMP:
                return new BasicNanoTimestamp(Utils.parseNanoTimestamp(value));
            default:
                throw new SQLException(srcClassName + " can not cast " + targetEntityClassName);
        }
    }

    public static Entity dataTime_java2db(Object srcValue, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcEntityClassName = srcValue.getClass().getName();

        switch (srcEntityClassName){
            case DATE:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicDate(((Date) srcValue).toLocalDate());
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case TIME:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicSecond(((Time) srcValue).toLocalTime());
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case TIMESTAMP:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicTimestamp(((Timestamp) srcValue).toLocalDateTime());
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case LOCAL_DATE:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicDate((LocalDate) srcValue);
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case LOCAL_TIME:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicSecond((LocalTime) srcValue);
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case LOCAL_DATETIME:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicTimestamp((LocalDateTime) srcValue);
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case YEAR_MONTH:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return new BasicMonth((YearMonth) srcValue);
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            default:
                return null;
        }

    }

    public static Entity dataTime_db2db(Entity srcEntity, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcEntityClassName = srcEntity.getClass().getName();
        switch (srcEntityClassName){
            case BASIC_MONTH:
            case BASIC_DATE:
            case BASIC_TIME:
            case BASIC_MINUTE:
            case BASIC_SECOND:
            case BASIC_NANOTIME:
            case BASIC_TIMESTAMP:
            case BASIC_DATETIME:
            case BASIC_NANOTIMESTAMP:
            case BASIC_MONTH_VECTOR:
            case BASIC_DATE_VECTOR:
            case BASIC_TIME_VECTOR:
            case BASIC_MINUTE_VECTOR:
            case BASIC_SECOND_VECTOR:
            case BASIC_NANOTIME_VECTOR:
            case BASIC_TIMESTAMP_VECTOR:
            case BASIC_DATETIME_VECTOR:
            case BASIC_NANOTIMESTAMP_VECTOR:
                switch (targetEntityClassName){
                    case BASIC_MONTH:
                    case BASIC_DATE:
                    case BASIC_TIME:
                    case BASIC_MINUTE:
                    case BASIC_SECOND:
                    case BASIC_NANOTIME:
                    case BASIC_TIMESTAMP:
                    case BASIC_DATETIME:
                    case BASIC_NANOTIMESTAMP:
                        return srcEntity;
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            default:
                return null;
        }

    }

    public static Entity basicTypeCast(Object srcValue, Entity targetEntity) throws SQLException{
        Entity castEntity;
        castEntity = basicType_java2db(srcValue,targetEntity);
        if(castEntity != null) return castEntity;
        if(srcValue instanceof Scalar || srcValue instanceof Vector) {
            castEntity = basicType_db2db((Entity) srcValue, targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof List){
            castEntity = basicType_db2db((Entity) srcValue,targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof Object[]){
            castEntity = basicTypeArr2Vector(srcValue,targetEntity);
            if (castEntity != null) return castEntity;
        }
        return null;
    }
    public static Entity DataTimeCast(Temporal temporal, String srcValue, Entity targetEntity){
        return null;
    }

    public static Entity dataTimeCast(Object srcValue, Entity targetEntity) throws SQLException{
        Entity castEntity;
        castEntity = basicType_java2db(srcValue,targetEntity);
        if(castEntity != null) return castEntity;
        if(srcValue instanceof Scalar || srcValue instanceof Vector) {
            castEntity = basicType_db2db((Entity) srcValue, targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof List){
            castEntity = basicType_db2db((Entity) srcValue,targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof Object[]){
            castEntity = basicTypeArr2Vector(srcValue,targetEntity);
            if (castEntity != null) return castEntity;
        }
        return null;
    }

    public static Entity basicTypeArr2Vector(Object srcValue,Entity targetEntity) throws SQLException{
        Object[] srcArr = (Object[])srcValue;
        int size = srcArr.length;
        if(size == 0){
            throw new SQLException(srcArr + "size can not 0 ");
        }
        Object srcValueFromArr = srcArr[0];
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcValueFromListClassName = srcValueFromArr.getClass().getName();
        if(srcValueFromArr instanceof Scalar){
            throw new SQLException("you need use com.xxdb.data.Vector load com.xxdb.data.Scalar");
        }
        switch (srcValueFromListClassName){
            case BOOLEAN:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicBooleanVector(size);
                        int index = 0;
                        for(boolean item : (boolean[]) srcValue){
                            ((BasicBooleanVector) targetVector).setBoolean(index,item);
                            ++index;
                        }
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);

                }
            case BYTE:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicByteVector((byte[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case CHAR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicByteVector(size);
                        int index = 0;
                        for(char item : (char[]) srcValue){
                            ((BasicByteVector) targetVector).setByte(index,(byte) (item & 0XFF));
                            ++index;
                        }
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case INT:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicIntVector((int[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case SHORT:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicShortVector((short[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case LONG:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicLongVector((long[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case FLOAT:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicFloatVector((float[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case DOUBLE:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        Vector targetVector = new BasicDoubleVector((double[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            case STRING:
                switch (targetEntityClassName){
                    case BASIC_STRING:
                        Vector targetVector = new BasicStringVector((String[]) srcValue);
                        return targetVector;
                    default:
                        throw new SQLException(srcValueFromListClassName + " can not cast to " + targetEntityClassName);
                }
            default:
                return null;
        }
    }


    public static Entity basicType_db2db(Entity srcEntity, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcEntityClassName = srcEntity.getClass().getName();
        switch (srcEntityClassName){
            case BASIC_BOOLEAN:
            case BASIC_BYTE:
            case BASIC_INT:
            case BASIC_SHORT:
            case BASIC_LONG:
            case BASIC_FLOAT:
            case BASIC_DOUBLE:
            case BASIC_BOOLEAN_VECTOR:
            case BASIC_BYTE_VECTOR:
            case BASIC_INT_VECTOR:
            case BASIC_SHORT_VECTOR:
            case BASIC_LONG_VECTOR:
            case BASIC_FLOAT_VECTOR:
            case BASIC_DOUBLE_VECTOR:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return srcEntity;
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            case BASIC_STRING:
            case BASIC_STRING_VECTOR:
                if(targetEntity instanceof BasicString || targetEntity instanceof BasicStringVector)
                    return srcEntity;
                else{
                    throw new SQLException(srcEntityClassName + " can not cast to " + targetEntityClassName);
                }
            default:
                return null;
        }

    }

    public static Entity basicType_java2db(Object srcValue, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcValueClassName = srcValue.getClass().getName();
        switch (srcValueClassName) {
            case BOOLEAN:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicBoolean((boolean) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case BYTE:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicByte((byte) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case CHAR:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicByte((byte) ((char) srcValue & 0xFF));
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case INT:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicInt((int) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case SHORT:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicShort((short) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case LONG:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicLong((long) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case FLOAT:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicFloat((float) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case DOUBLE:
                switch (targetEntityClassName) {
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return new BasicDouble((double) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast  " + targetEntityClassName);
                }
            case STRING:
                switch (targetEntityClassName) {
                    case BASIC_STRING:
                        return new BasicString((String) srcValue);
                    default:
                        throw new SQLException(srcValueClassName + " can not cast to " + targetEntityClassName);
                }
            default:
                return null;
        }
    }
}
