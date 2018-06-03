package com.dolphindb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * 协议: jdbc:dolphindb://hostName:port?databasePath=
 *      或者 jdbc:dolphindb://databasePath=
 * hostName default localhost
 * port default 8848
 * linux url示例 ==> jdbc:dolphindb://127.0.0.1:8848?databasePath=home/username/dolphinDB/data/db01/t1
 * windows url示例 ==> jdbc:dolphindb://127.0.0.1:8848?databasePath=D:/dolphinDB/data/db01/t1
 */

public class Driver implements java.sql.Driver {
    private static final String URL_PREFIX = "jdbc:dolphindb://";
    public  static final String DB = "system_db";
    static int V=1,v=0;
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        return createConnection(url, properties);
    }

    @Override
    public boolean acceptsURL(String s) throws SQLException {
        return isValidURL(s);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return V;
    }

    @Override
    public int getMinorVersion() {
        return v;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    public static boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(URL_PREFIX);
    }


    public Connection createConnection(String url, Properties prop) throws SQLException {
        if (!isValidURL(url)) new SQLException("url is not valid");
        System.out.println(url);
        String old_url = url;
        url = url.trim().substring(URL_PREFIX.length());
        if(url.length()==0){
            prop.setProperty("hostName","localhost");
            prop.setProperty("port","8848");
            return new JDBCConnection(prop);
        }else{
            if(!url.contains("?"))  throw new SQLException(old_url + " is error\n we need ? like that jdbc:dolphindb://?databasePath= or jdbc:dolphindb://hostName:port?databasePath=");
            if(url.startsWith("?")){
                prop.setProperty("hostName", "localhost");
                prop.setProperty("port", "8848");
                String[] strings = url.split("\\?");
                Utils.parseProperties(strings[1],prop,"&","=");
            }else{
                String[] strings = url.split("\\?");
                String[] hostname_port = strings[0].split(":");
                if(hostname_port.length ==2) {
                    prop.setProperty("hostName", hostname_port[0]);
                    prop.setProperty("port", hostname_port[1]);
                }else{
                    throw new SQLException("hostname_port " + strings[0] + " error");
                }
                if(strings.length > 0){
                    Utils.parseProperties(strings[1],prop,"&","=");
                }
            }
            return new JDBCConnection(prop);
        }
    }

    public static void unused(String s)throws SQLException{
        throw new SQLException(s);
    }
    public static void unused()throws SQLException{
        throw new SQLFeatureNotSupportedException("NotSupported");
    }
    public static void unused(Exception e)throws SQLException{
        throw new SQLException(e.getMessage());
    }
}
