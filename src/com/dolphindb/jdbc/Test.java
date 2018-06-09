package com.dolphindb.jdbc;

import com.xxdb.data.BasicDate;
import com.xxdb.data.BasicNanoTimestamp;
import com.xxdb.data.Utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) throws  Exception{
//        String regex = "full.*join|inner.*join|right.*join|left.*join|" +
//                "join.*(.*)|ej.*(.*)|sej.*(.*)|lj.*(.*)|fj.*(.*)|aj.*(.*)|cj.*(.*)|" +
//                "group.*by|context.*by|pivot.*by";
//        String s = "select bool,char from e(t1, t1, `bool)";
//        Pattern pattern = Pattern.compile(regex);
//        Matcher matcher = pattern.matcher(s);
//        System.out.println(matcher.find());
//        DBConnection connection = new DBConnection();
//        connection.connect("127.0.0.1",8848);
        //BasicTable basicTable = (BasicTable) connection.run(MessageFormat.format("TABLE_CAT=(`{0},`{1});table(TABLE_CAT)", com.dolphindb.jdbc.Driver.DB,DATABASE_NAME));
        //ResultSet resultSet = new JDBCResultSet(connection,null,basicTable,"");
//        System.out.println(Driver.SYSTEM_PROPS.getProperty("os.name") + Driver.SYSTEM_PROPS.getProperty("os.version"));
//
//        try {
//            BasicTable table = (BasicTable) connection.run("defs()");
//            StringBuilder sb = new StringBuilder();
//            int rows = table.rows();
//            for(int i=0; i<rows; ++i){
//                sb.append(table.getColumn(0).get(i)).append(",");
//            }
//            System.out.println(sb.toString());
//        }catch (Exception e){
//            e.printStackTrace();
//        }

//        List<Integer> integers = new ArrayList<>(10);
//        integers.add(0);
//        for(int i =0; i < 10; ++i){
//            integers.set(i,i);
//        }
        System.out.println(Date.class.getName());
        System.out.println(Time.class.getName());
        System.out.println(Timestamp.class.getName());
        System.out.println(LocalTime.class.getName());
        System.out.println(LocalDate.class.getName());
        System.out.println(LocalDateTime.class.getName());
        System.out.println(YearMonth.class.getName());

        System.out.println(Boolean.class.getName());
        System.out.println(Byte.class.getName());
        System.out.println(Character.class.getName());
        System.out.println(Short.class.getName());
        System.out.println(Integer.class.getName());
        System.out.println(Long.class.getName());
        System.out.println(Float.class.getName());
        System.out.println(Double.class.getName());
        System.out.println(String.class.getName());

        List<String> strings = new ArrayList<>();

        System.out.println(strings.getClass().getName());

//        long l = 1000000;
//
//        Date date = new Date(l);
//        Time time = new Time(l);
//        Timestamp timestamp = new Timestamp(l);
//        System.out.println(date.toLocalDate());
//        System.out.println(time.toLocalTime());
//        System.out.println(timestamp.toLocalDateTime());

        BasicDate basicDate = new BasicDate(LocalDate.parse("2013-06-14"));
        BasicNanoTimestamp basicNanoTimestamp = new BasicNanoTimestamp(Utils.parseNanoTimestamp(TypeCast.L+basicDate.getInt()));
        LocalDate localDate = basicDate.getDate();
        //LocalDateTime localDateTime = LocalDateTime.of(localDate.getYear(),localDate.getMonthValue(),localDate.getDayOfMonth(),0,0,0);
        System.out.println(basicDate.getInt());
        LocalDateTime localDateTime = LocalDateTime.of(1988,1,1,0,0,0,1);
        System.out.println(localDateTime.getYear());
        long l = Utils.countMilliseconds(LocalDateTime.of(1970,1,1,1,0,0,1));
        System.out.println(l);
        System.out.println(basicNanoTimestamp.getString());

//        LocalDateTime dateTime = LocalDateTime.now();
//        String[] strings1 = new String[]{TypeCast.YEAR_MONTH,TypeCast.LOCAL_TIME,TypeCast.LOCAL_DATE,TypeCast.LOCAL_DATETIME};
//        Temporal[] temporals = new Temporal[strings1.length];
//        int index =0;
//        for(String s:strings1){
//            temporals[index] = TypeCast.castTemporal(dateTime,s);
//            System.out.println(temporals[index]);
//            ++index;
//        }
//        for(int i=0,ilen =strings1.length;i<ilen; ++i){
//            System.out.println(strings1[i]);
//            for (int j=0,jlen =strings1.length;j<jlen; ++j){
//                System.out.println(strings1[j]);
//                System.out.println(TypeCast.castTemporal(temporals[i],strings1[j]));
//            }
//        }
    }
}
