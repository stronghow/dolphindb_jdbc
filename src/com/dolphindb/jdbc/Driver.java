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
        url = url.trim().substring(URL_PREFIX.length());
        if(url.length()==0||url.equals("?")){
            prop.setProperty("hostName","localhost");
            prop.setProperty("port","8848");
            return new JDBCConnection(prop);
        }else{
            String[] strings = url.split("\\?");
            if(strings.length == 1){
                String s = strings[0];
                if(s.length() >0){
                    if(s.contains("=")){
                        prop.setProperty("hostName", "localhost");
                        prop.setProperty("port", "8848");
                        Utils.parseProperties(s,prop,"&","=");
                    }else{
                        String[] hostname_port = s.split(":");
                        if(hostname_port.length ==2) {
                            prop.setProperty("hostName", hostname_port[0]);
                            prop.setProperty("port", hostname_port[1]);
                        }else{
                            throw new SQLException("hostname_port " + strings[0] + " error");
                        }
                    }
                }
            }else if(strings.length == 2){
                String s1 = strings[0];
                if(s1.length()>0){
                    String[] hostname_port = s1.split(":");
                    if(hostname_port.length ==2) {
                        prop.setProperty("hostName", hostname_port[0]);
                        prop.setProperty("port", hostname_port[1]);
                    }else{
                        throw new SQLException("hostname_port " + strings[0] + " error");
                    }
                }else{
                    prop.setProperty("hostName", "localhost");
                    prop.setProperty("port", "8848");
                }
                String s2 = strings[1];
                if(s2.length()>0){
                    Utils.parseProperties(s2,prop,"&","=");
                }
            }
            return new JDBCConnection(prop);
        }
    }

    private void parseUrl(String url){

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
