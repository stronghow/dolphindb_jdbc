package com.dolphindb.jdbc;

import com.xxdb.data.*;

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

    public static final int YEAR = 1970;
    public static final int MONTH = 1;
    public static final int DAY = 1;
    public static final int HOUR = 0;
    public static final int MINUTE = 0;
    public static final int SECOND = 0;
    public static final int NANO = 0;

    public static final YearMonth YEARMONTH = YearMonth.of(YEAR,MONTH);
    public static final LocalTime LOCALTIME = LocalTime.of(HOUR,MINUTE,SECOND,NANO);
    public static final LocalDate LOCALDATE = LocalDate.of(YEAR,MONTH,DAY);
    public static final LocalDateTime LOCALDATETIME = LocalDateTime.of(LOCALDATE,LOCALTIME);


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

    public static Entity java2db(Object srcValue, Entity targetEntity) throws Exception{
        String srcValueClassName = srcValue.getClass().getName();
        String targetEntityClassName = targetEntity.getClass().getName();
        Entity castEntity = null;
        if(srcValueClassName.equals(targetEntityClassName) || srcValueClassName.startsWith(targetEntityClassName)){
            return (Entity) srcValue;
        }
        castEntity = dateTimeCast(srcValue,targetEntity);
        if(castEntity != null) return castEntity;
        castEntity = basicTypeCast(srcValue,targetEntity);
        if(castEntity != null) return castEntity;
        throw new SQLException("only support bool byte char short int long float double Date Time Timestamp YearMoth LocalDate LocalTime LocalDateTime Scalar Vector");
    }

    public static Entity dateTimeCast(Object srcValue, Entity targetEntity) throws Exception{
        Entity castEntity;
        castEntity = dataTime_java2db(srcValue,targetEntity);
        if(castEntity != null) return castEntity;
        if(srcValue instanceof Scalar || srcValue instanceof Vector) {
            castEntity = dateTime_db2db((Entity) srcValue, targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof List){
            castEntity = dateTimeArr2Vector(((List) srcValue).toArray(),targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof Object[]){
            castEntity = dateTimeArr2Vector(srcValue,targetEntity);
            if (castEntity != null) return castEntity;
        }
        return null;
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
            castEntity = basicTypeArr2Vector(((List) srcValue).toArray(),targetEntity);
            if (castEntity != null) return castEntity;
        }
        if(srcValue instanceof Object[]){
            castEntity = basicTypeArr2Vector(srcValue,targetEntity);
            if (castEntity != null) return castEntity;
        }
        return null;
    }

    public static Entity dataTime_java2db(Object srcValue, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcEntityClassName = srcValue.getClass().getName();
        if(!CheckedDateTime(srcEntityClassName,targetEntityClassName)) {
            return null;
        }
        Temporal temporal = null;
        switch (srcEntityClassName){
            case DATE:
                temporal = ((Date) srcValue).toLocalDate();
                break;
            case TIME:
                temporal = ((Time) srcValue).toLocalTime();
                break;
            case TIMESTAMP:
                temporal = ((Timestamp) srcValue).toLocalDateTime();
                break;
            case LOCAL_DATE:
                temporal = (LocalDate)srcValue;
                break;
            case LOCAL_TIME:
                temporal = (LocalTime)srcValue;
                break;
            case LOCAL_DATETIME:
                temporal = (LocalDateTime)srcValue;
                break;
            case YEAR_MONTH:
                temporal = (YearMonth)srcValue;
                break;
            default:
                return null;
        }

        return Temporal2dateTime(temporal,srcEntityClassName,targetEntityClassName);
    }

    public static boolean CheckedDateTime(String srcEntityClassName, String targetEntityClassName) throws SQLException{
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
            case DATE:
            case TIME:
            case TIMESTAMP:
            case YEAR_MONTH:
            case LOCAL_TIME:
            case LOCAL_DATE:
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
                        return true;
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            default:
                return false;
        }
    }

    public static Entity Tempos2dateTime(Object srcTempos, String srcEntityClassName, String targetEntityClassName) throws SQLException{
        Object[] objects = (Object[]) srcTempos;
        int size = objects.length;
        switch (targetEntityClassName) {
            case BASIC_MONTH: {
                BasicMonthVector targetVector = new BasicMonthVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setMonth(index, (YearMonth) castTemporal(srcTemporal, YEAR_MONTH));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_DATE:{
                BasicDateVector targetVector = new BasicDateVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setDate(index, (LocalDate) castTemporal(srcTemporal, LOCAL_DATE));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_TIME:{
                BasicTimeVector targetVector = new BasicTimeVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setTime(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_MINUTE:{
                BasicMinuteVector targetVector = new BasicMinuteVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setMinute(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_SECOND:{
                BasicSecondVector targetVector = new BasicSecondVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setSecond(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_NANOTIME:{
                BasicNanoTimeVector targetVector = new BasicNanoTimeVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setNanoTime(index, (LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_TIMESTAMP: {
                BasicTimestampVector targetVector = new BasicTimestampVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setTimestamp(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_DATETIME:{
                BasicDateTimeVector targetVector = new BasicDateTimeVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setDateTime(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            case BASIC_NANOTIMESTAMP:{
                BasicNanoTimestampVector targetVector = new BasicNanoTimestampVector(size);
                int index = 0;
                for (Object srcTemporal : objects) {
                    targetVector.setNanoTimestamp(index, (LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
                    ++index;
                }
                return targetVector;
            }
            default:
                throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
        }
    }

    public static Entity Temporal2dateTime(Temporal srcTemporal, String srcEntityClassName, String targetEntityClassName) throws SQLException{
        switch (targetEntityClassName) {
            case BASIC_MONTH:
                return new BasicMonth((YearMonth) castTemporal(srcTemporal,YEAR_MONTH));
            case BASIC_DATE:
                return new BasicDate((LocalDate) castTemporal(srcTemporal,LOCAL_DATE));
            case BASIC_TIME:
                return new BasicTime((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_MINUTE:
                return new BasicMinute((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_SECOND:
                return new BasicSecond((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_NANOTIME:
                return new BasicNanoTime((LocalTime) castTemporal(srcTemporal,LOCAL_TIME));
            case BASIC_TIMESTAMP:
                return new BasicTimestamp((LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
            case BASIC_DATETIME:
                return new BasicDateTime((LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
            case BASIC_NANOTIMESTAMP:
                return new BasicNanoTimestamp((LocalDateTime) castTemporal(srcTemporal,LOCAL_DATETIME));
            default:
                throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
        }
    }

    public static Entity dateTime_db2db(Entity srcEntity, Entity targetEntity) throws Exception{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcEntityClassName = srcEntity.getClass().getName();
        if(!CheckedDateTime(srcEntityClassName,targetEntityClassName))
            return null;
        Temporal srcTemporal = null;
        Temporal[] srcTempos = null;
        switch (srcEntityClassName){
            case BASIC_MONTH:
                srcTemporal = ((BasicMonth)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_DATE:
                srcTemporal = ((BasicDate)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_TIME:
                srcTemporal = ((BasicTime)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_MINUTE:
                srcTemporal = ((BasicMinute)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_SECOND:
                srcTemporal = ((BasicSecond)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_NANOTIME:
                srcTemporal = ((BasicNanoTime)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_TIMESTAMP:
                srcTemporal = ((BasicTimestamp)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_DATETIME:
                srcTemporal = ((BasicDateTime)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_NANOTIMESTAMP:
                srcTemporal = ((BasicNanoTimestamp)srcEntity).getTemporal();
                return Temporal2dateTime(srcTemporal,srcEntityClassName,targetEntityClassName);
            case BASIC_MONTH_VECTOR: {
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicMonthVector) srcEntity).getMonth(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_DATE_VECTOR: {
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicDateVector) srcEntity).getDate(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_TIME_VECTOR: {
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicTimeVector) srcEntity).getTime(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_MINUTE_VECTOR:{
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicMinuteVector) srcEntity).getMinute(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_SECOND_VECTOR:{
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicSecondVector) srcEntity).getSecond(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_NANOTIME_VECTOR:{
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicNanoTimeVector) srcEntity).getNanoTime(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_TIMESTAMP_VECTOR:{
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicTimestampVector) srcEntity).getTimestamp(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_DATETIME_VECTOR:{
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicDateTimeVector) srcEntity).getDateTime(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            case BASIC_NANOTIMESTAMP_VECTOR:{
                int size = srcEntity.rows();
                srcTempos = new Temporal[size];
                for (int i = 0; i < size; ++i) {
                    srcTempos[i] = ((BasicNanoTimestampVector) srcEntity).getNanoTimestamp(i);
                }
                return Tempos2dateTime(srcTempos, srcEntityClassName, targetEntityClassName);
            }
            default:
                return null;
        }

    }

    public static Entity dateTimeArr2Vector(Object srcValue,Entity targetEntity) throws SQLException{
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
        if(!CheckedDateTime(srcValueFromListClassName,targetEntityClassName)) return null;
        return Tempos2dateTime(srcArr,srcValueFromListClassName,targetEntityClassName);
    }

    public static boolean CheckedBasicType(String srcEntityClassName, String targetEntityClassName) throws SQLException{
        switch (srcEntityClassName){
            case BASIC_BOOLEAN:
            case BASIC_BYTE:
            case BASIC_INT:
            case BASIC_SHORT:
            case BASIC_LONG:
            case BASIC_FLOAT:
            case BASIC_DOUBLE:
            case BASIC_STRING:
            case BASIC_BOOLEAN_VECTOR:
            case BASIC_BYTE_VECTOR:
            case BASIC_INT_VECTOR:
            case BASIC_SHORT_VECTOR:
            case BASIC_LONG_VECTOR:
            case BASIC_FLOAT_VECTOR:
            case BASIC_DOUBLE_VECTOR:
            case BASIC_STRING_VECTOR:
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
                switch (targetEntityClassName){
                    case BASIC_BOOLEAN:
                    case BASIC_BYTE:
                    case BASIC_INT:
                    case BASIC_SHORT:
                    case BASIC_LONG:
                    case BASIC_FLOAT:
                    case BASIC_DOUBLE:
                    case BASIC_STRING:
                        return true;
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast " + targetEntityClassName);
                }
            default:
                return false;
        }
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
        if(!CheckedBasicType(srcValueFromListClassName,targetEntityClassName)) return null;

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

        if(!CheckedBasicType(srcEntityClassName,targetEntityClassName)) return null;

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
                switch (targetEntityClassName){
                    case BASIC_STRING:
                        return srcEntity;
                    default:
                        throw new SQLException(srcEntityClassName + " can not cast to " + targetEntityClassName);
                }
            default:
                return null;
        }

    }

    public static Entity basicType_java2db(Object srcValue, Entity targetEntity) throws SQLException{
        String targetEntityClassName = targetEntity.getClass().getName();
        String srcValueClassName = srcValue.getClass().getName();

        if(!CheckedBasicType(srcValueClassName,targetEntityClassName)) return null;

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

    public static Temporal castTemporal(Object srcValue,String targetTemporalClassName){
        String srcTemporalClassName = srcValue.getClass().getName();
        Temporal srcTemporal = null;
        switch (srcTemporalClassName){
            case DATE:
                srcTemporal = ((Date)srcValue).toLocalDate();
                srcTemporalClassName = srcTemporal.getClass().getName();
                break;
            case TIME:
                srcTemporal = ((Time)srcValue).toLocalTime();
                srcTemporalClassName = srcTemporal.getClass().getName();
                break;
            case TIMESTAMP:
                srcTemporal = ((Timestamp)srcValue).toLocalDateTime();
                srcTemporalClassName = srcTemporal.getClass().getName();
                break;
            default:
                srcTemporal = (Temporal)srcValue;
        }
        switch (targetTemporalClassName){
            case YEAR_MONTH:
                switch (srcTemporalClassName){
                    case LOCAL_TIME:
                        return YEARMONTH;
                    case LOCAL_DATE:
                        LocalDate localDate = (LocalDate)srcTemporal;
                        return YearMonth.of(localDate.getYear(),localDate.getMonthValue());
                    case LOCAL_DATETIME:
                        LocalDateTime localDateTime = (LocalDateTime)srcTemporal;
                        return YearMonth.of(localDateTime.getYear(),localDateTime.getMonthValue());
                    default:
                        return srcTemporal;

                }
            case LOCAL_DATE:
                switch (srcTemporalClassName){
                    case YEAR_MONTH:
                        return  ((YearMonth) srcTemporal).atEndOfMonth();
                    case LOCAL_TIME:
                        return LOCALDATE;
                    case LOCAL_DATETIME:
                        return  ((LocalDateTime)srcTemporal).toLocalDate();
                    default:
                        return srcTemporal;

                }
            case LOCAL_TIME:
                switch (srcTemporalClassName){
                    case YEAR_MONTH:
                        return LOCALTIME;
                    case LOCAL_DATE:
                        return LOCALTIME;
                    case LOCAL_DATETIME:
                        return ((LocalDateTime)srcTemporal).toLocalTime();
                    default:
                        return srcTemporal;
                }
            case LOCAL_DATETIME:
                switch (srcTemporalClassName){
                    case YEAR_MONTH:
                        return ((YearMonth)srcTemporal).atEndOfMonth().atStartOfDay();
                    case LOCAL_DATE:
                        return  ((LocalDate)srcTemporal).atStartOfDay();
                    case LOCAL_TIME:
                        LocalTime localTime = (LocalTime)srcTemporal;
                        return LocalDateTime.of(YEAR,MONTH,DAY,localTime.getHour(),localTime.getMinute(),localTime.getSecond(),localTime.getNano());
                    default:
                        return srcTemporal;
                }
            default:
                return srcTemporal;
        }
    }
}
