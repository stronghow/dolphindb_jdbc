package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;

import java.text.MessageFormat;

import static com.dolphindb.jdbc.JDBCDataBaseMetaData.DATABASE_NAME;

public class Test {
    public static void main(String[] args) throws  Exception{
//        String regex = "full.*join|inner.*join|right.*join|left.*join|" +
//                "join.*(.*)|ej.*(.*)|sej.*(.*)|lj.*(.*)|fj.*(.*)|aj.*(.*)|cj.*(.*)|" +
//                "group.*by|context.*by|pivot.*by";
//        String s = "select bool,char from e(t1, t1, `bool)";
//        Pattern pattern = Pattern.compile(regex);
//        Matcher matcher = pattern.matcher(s);
//        System.out.println(matcher.find());
        DBConnection connection = new DBConnection();
        connection.connect("127.0.0.1",8848);
        BasicTable basicTable = (BasicTable) connection.run(MessageFormat.format("TABLE_CAT=(`{0},`{1});table(TABLE_CAT)", com.dolphindb.jdbc.Driver.DB,DATABASE_NAME));
        //ResultSet resultSet = new JDBCResultSet(connection,null,basicTable,"");
    }
}
