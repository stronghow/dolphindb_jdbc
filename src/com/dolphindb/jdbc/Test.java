package com.dolphindb.jdbc;

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

        //Main.TestTypeCast(4);
        boolean[] booleans = new boolean[]{true};
        byte[] bytes = new byte[]{(byte)97};
        char[] chars = new char[]{'a'};
        short[] shorts = new short[]{(short)1};
        int[] ints = new int[]{1};
        long[] longs = new long[]{1l};
        float[] floats = new float[]{1.0f};
        double[] doubles = new double[]{1.0};
        String[] strings1 = new String[]{"1"};

        System.out.println(booleans instanceof boolean[]);

        System.out.println(boolean[].class.getName());
        System.out.println(byte[].class.getName());
        System.out.println(char[].class.getName());
        System.out.println(short[].class.getName());
        System.out.println(int[].class.getName());
        System.out.println(long[].class.getName());
        System.out.println(float[].class.getName());
        System.out.println(double[].class.getName());

        System.out.println(Boolean[].class.getName());
        System.out.println(Byte[].class.getName());
        System.out.println(Character[].class.getName());
        System.out.println(Short[].class.getName());
        System.out.println(Integer[].class.getName());
        System.out.println(Long[].class.getName());
        System.out.println(Float[].class.getName());
        System.out.println(Double[].class.getName());
        System.out.println(String[].class.getName());

        System.out.println(strings1 instanceof Object[]);

    }
}
