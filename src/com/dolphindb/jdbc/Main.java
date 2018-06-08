package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {
    private static final String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";

    private static final String path_All = "/data/dballdata";

    private static final String path_test = "/data/test";

    private static final String DB_URL = MessageFormat.format("jdbc:dolphindb://localhost:8848?databasePath={0}{1}",System.getProperty("user.dir").replaceAll("\\\\","/"),path_All);

    private static final String DB_URL1 = "jdbc:dolphindb://";

    private static final String DB_URL_DFS = "jdbc:dolphindb://localhost:8499?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2016.12M";

    private static final String DB_URL_DFS1 = "jdbc:dolphindb://localhost:8499?databasePath=dfs://rangedb&partitionType=RANGE&partitionScheme= 0 5 10&locations= [`rh8503, `rh8502`rh8504]";

    public static void main(String[] args) throws Exception{

       //CreateTable(System.getProperty("user.dir")+"/data/createTable_all.java",path_All,"t2");



        Object[] o1 = new Object[]{true, 'a', 122, 21, 22, 2.1f, 2.1, "Hello",
                new BasicDate(LocalDate.parse("2013-06-13")),
                new BasicMonth(YearMonth.parse("2016-06")),
                new BasicTime(LocalTime.parse("13:30:10.008")),
                new BasicMinute(LocalTime.parse("13:30:10")),
                new BasicSecond(LocalTime.parse("13:30:10")),
                new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:10")),
                new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008")),
                new BasicNanoTime(LocalTime.parse("13:30:10.008007006")),
                new BasicDate(LocalDate.parse("2013-06-13"))};

        Object[] o2 = new Object[]{true, 'A', 123, 22, 23, 2.2f, 2.2, "world",
                new BasicDate(LocalDate.parse("2013-06-14")),
                new BasicMonth(YearMonth.parse("2016-07")),
                new BasicTime(LocalTime.parse("13:30:10.009")),
                new BasicMinute(LocalTime.parse("13:31:10")),
                new BasicSecond(LocalTime.parse("13:30:11")),
                new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:11")),
                new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.009")),
                new BasicNanoTime(LocalTime.parse("13:30:10.008007007")),
                new BasicDate(LocalDate.parse("2013-06-14"))};

        Object[][] o3 = new Object[][]{o1,o2};

        int len = o3[0].length;
        int n = o3.length;

        HashMap<Integer,Object> map = new HashMap<>(len+1);
        Object[] o4 = new Object[len];
        Vector vector;

        for(int i=0; i< len; ++i){
            vector = new BasicAnyVector(n);
            switch (i){
                case 0:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicBoolean((boolean)o3[j][i]));
                    }
                    break;
                case 1:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicByte((byte) ((char)o3[j][i] & 0xFF)));
                    }
                    break;
                case 2:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicShort(Short.parseShort(o3[j][i].toString())));
                    }
                    break;
                case 3:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicInt((int)o3[j][i]));
                    }
                    break;
                case 4:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicLong((int)o3[j][i]));
                    }
                    break;
                case 5:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicFloat((float) o3[j][i]));
                    }
                    break;
                case 6:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicDouble((double)o3[j][i]));
                    }
                    break;
                case 7:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicString((String) o3[j][i]));
                    }
                    break;
                case 8:
                case 9:
                case 10:
                case 11:
                case 12:
                case 13:
                case 14:
                case 15:
                case 16:
                    for(int j=0; j<n; ++j){
                        vector.set(j,(Scalar) o3[j][i]);
                    }
                    break;
            }
            map.put(i+1,vector);
            o4[i] = vector;
        }


//        TestPreparedStatement(DB_URL,"t1 = loadTable(system_db,`t1)","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o4);
//
//
//        TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
//        TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,true);
//
//        TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select bool,char from ej(t1, t1, `bool)",new Object[]{true,'a'},true);
//
//        TestResultSetUpdate(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
//        TestResultSetUpdate(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,true);
//
//        TestResultSetDelete(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",2,false);
//        TestResultSetDelete(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",2,true);

//        TestPreparedStatementBatch(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o3);
//
//
//
//        TestResultSetInsert(DB_URL_DFS,"pt = loadTable(system_db,`pt)","select top 10 * from pt",new Object[]{new BASIC_MONTH(YearMonth.parse("2016-07")),0.007},true);
//
//        TestResultSetUpdate(DB_URL_DFS,"pt = loadTable(system_db,`pt)","select top 10 * from pt",new Object[]{new BASIC_MONTH(YearMonth.parse("2016-07")),0.007},true);
//
//        TestResultSetDelete(DB_URL_DFS,"pt = loadTable(system_db,`pt)","select top 10 * from pt",1,true);
//
//
//        String sql = "bool = [true, false];\n" +
//                "int = [1, 2];\n" +
//                "t1 = table(bool, int);\n" +
//                "insert into t1 values (bool, int);\n";
//        TestResultSetUpdate(DB_URL,sql,"select * from t1",new Object[]{true, 3},true);
//
//        TestStatementExecute(DB_URL_DFS1,"select  top 10 * from pt");

//        TestDatabaseMetaData(DB_URL1,"");

        TestPreparedStatementInsert();


    }

    public static void CreateTable(String file,String savePath,String tableName){
        DBConnection db=null;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            StringBuilder sb = new StringBuilder();
            String s = null;
            while ((s = bufferedReader.readLine()) != null){
                sb.append(s).append("\n");
            }
            sb.append(Driver.DB).append(" = ").append("database(\"").
                    append(System.getProperty("user.dir").replaceAll("\\\\","/")).append(savePath).append("\")\n");
            sb.append("saveTable(").append(Driver.DB).append(", t1, `").append(tableName).append(");\n");
            db = new DBConnection();
            db.connect("127.0.0.1",8848);
            db.run(sb.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(db != null) db.close();
        }
    }



    public static void TestStatementExecute(String url,String sql) throws Exception{
        System.out.println("TestStatementExecute begin");
        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            ResultSet rs = null;
            int UpdateCount = -1;
            if(stmt.execute(sql)){
                rs = stmt.getResultSet();
                printData(rs);
            }else {
                UpdateCount =  stmt.getUpdateCount();
                if(UpdateCount != -1) {
                    System.out.println(UpdateCount + " row affected");
                }
            }
            while (true){
                if(stmt.getMoreResults()){
                    rs =  stmt.getResultSet();
                    printData(rs);
                }else{
                    UpdateCount =  stmt.getUpdateCount();
                    if(UpdateCount != -1) {
                        System.out.println(UpdateCount + "row affected");
                    }else{
                        break;
                    }
                }
            }
            if(rs != null) {
                rs.close();
            }
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestStatementExecute end");
    }


    public static void TestPreparedStatement(String url,String loadTable,String preSql, Object[] objects) throws Exception{
        System.out.println("TestStatement begin");

        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.prepareStatement(preSql);
            stmt.execute(loadTable);
            int index = 1;
            for(Object o: objects){
                stmt.setObject(index,o);
                ++index;
            }
            ResultSet rs = null;
            int UpdateCount = -1;
            if(stmt.execute()){
                rs = stmt.getResultSet();
                printData(rs);
            }else {
                UpdateCount =  stmt.getUpdateCount();
                if(UpdateCount != -1) {
                    System.out.println(UpdateCount + " row affected");
                }
            }
            while (true){
                if(stmt.getMoreResults()){
                    rs =  stmt.getResultSet();
                    printData(rs);
                }else{
                    UpdateCount =  stmt.getUpdateCount();
                    if(UpdateCount != -1) {
                        System.out.println(UpdateCount + "row affected");
                    }else{
                        break;
                    }
                }
            }
            if(rs != null) {
                rs.close();
            }
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestPreparedStatement end");
    }

    public static void TestPreparedStatementBatch(String url,String loadTable, String select, String batchSql, Object[] objects) throws Exception{
        System.out.println("TestPreparedStatementBatch begin");

        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.prepareStatement(batchSql);
            stmt.execute(loadTable);
            for(int i=0; i<objects.length; ++i){
                int index = 1;
                Object[] o = (Object[]) objects[i];
                for(Object o1: o){
                    stmt.setObject(index,o1);
                    ++index;
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
            ResultSet rs = stmt.executeQuery(select);
            printData(rs);
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            se.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestPreparedStatementBatch end");
    }

    public static void TestResultSetInsert(String url, String loadTable, String select, Object[] objects, boolean isInsert) throws Exception{
        System.out.println("TestResultSetInsert begin");

        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute(loadTable);
            ResultSet rs = stmt.executeQuery(select);
            printData(rs);
            rs.absolute(2);
            rs.moveToInsertRow();
            int index = 1;
            for(Object o : objects){
                rs.updateObject(index,o);
                ++index;
            }
            if(isInsert) {
                rs.insertRow();
            }
            rs.beforeFirst();
            printData(rs);
            rs.close();
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestResultSetInsert end");
    }

    public static void TestResultSetUpdate(String url,String loadTable,String select, Object[] objects, boolean isUpdate) throws Exception{
        System.out.println("TestResultSetInsert begin");

        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute(loadTable);
            ResultSet rs = stmt.executeQuery(select);
            printData(rs);
            rs.absolute(2);
            int index = 1;
            for(Object o : objects){
                rs.updateObject(index,o);
                ++index;
            }
            if(isUpdate) {
                rs.updateRow();
            }
            rs.beforeFirst();
            printData(rs);
            rs.close();
            stmt.close();
            conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestResultSetInsert end");
    }

    public static void TestResultSetDelete(String url,String loadTable,String select, int row, boolean isDelete) throws Exception{
        System.out.println("TestResultSetDelete begin");

        Connection conn = null;
        Statement stmt = null;
        try{

            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute(loadTable);
            ResultSet rs = stmt.executeQuery(select);
            printData(rs);
            rs.absolute(row);
            if(isDelete) {
                rs.deleteRow();
            }
            rs.beforeFirst();
            printData(rs);
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestResultSetDelete end");
    }

    public static void TestDatabaseMetaData(String url,String sql) throws Exception{
        System.out.println("TestStatementExecute begin");
        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            ResultSet rs = null;
            DatabaseMetaData metaData = conn.getMetaData();
            rs = metaData.getCatalogs();
            printData(rs);
            String s = metaData.getCatalogSeparator();
            System.out.println(s);
            s = metaData.getCatalogTerm();
            System.out.println(s);
            s = metaData.getDatabaseProductName();
            System.out.println(s);
            s = metaData.getDatabaseProductVersion();
            System.out.println(s);
            s = metaData.getDriverName();
            System.out.println(s);
            System.out.println(metaData.getDriverVersion());
            System.out.println(metaData.getExtraNameCharacters());
            System.out.println(metaData.getIdentifierQuoteString());
            System.out.println(metaData.getJDBCMajorVersion());
            System.out.println(metaData.getJDBCMinorVersion());
            System.out.println(metaData.getMaxBinaryLiteralLength());
            System.out.println(metaData.getMaxCatalogNameLength());
            System.out.println(metaData.getMaxColumnNameLength());
            System.out.println(metaData.getNumericFunctions());
            System.out.println(metaData.getProcedureTerm());
            System.out.println(metaData.getResultSetHoldability());
            System.out.println(metaData.getSchemaTerm());
            System.out.println(metaData.getSearchStringEscape());
            System.out.println("getSQLKeywords() = "+metaData.getSQLKeywords());
            System.out.println("getSQLStateType() = " +metaData.getSQLStateType());
            System.out.println("getStringFunctions() =" +metaData.getStringFunctions());
            System.out.println("getSystemFunctions() = " +metaData.getSystemFunctions());
            printData(metaData.getTableTypes());
            System.out.println("getTimeDateFunctions() = " + metaData.getTimeDateFunctions());
            printData(metaData.getTypeInfo());
            System.out.println("getURL() = " + metaData.getURL());
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestStatementExecute end");
    }

    public static void printData(ResultSet rs) throws SQLException{
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int len = resultSetMetaData.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= len; ++i) {
                System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
            }
            System.out.print("\n");
        }
    }

    public static void TestPreparedStatementInsert() throws Exception{
        System.out.println("TestStatement begin");

        Connection conn = null;
        PreparedStatement stmt = null;

        DBConnection dbConnection = new DBConnection();
        dbConnection.connect("127.0.0.1",8848);
        BasicIntVector entity1 = (BasicIntVector)dbConnection.run("1..10000000");
        List<Entity> entities = new ArrayList<>(5);
        //entities.add(new BasicString("t1"));
        for(int i =0; i< 4; i++){
            entities.add(entity1);
        }

//        dbConnection.run("t1 = table(1 as a, 2 as b, 3 as c, 4 as d)");
//        dbConnection.run("tableInsert",entities);



        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL1);
            stmt = conn.prepareStatement("tableInsert(t1,?,?,?,?)");
            stmt.execute("t1 = table(1 as a, 2 as b, 3 as c, 4 as d)");
            int index = 1;
            for(Entity entity: entities){
                stmt.setObject(index,entity);
                ++index;
            }
            ResultSet rs = null;
            int UpdateCount = -1;
            long time = System.currentTimeMillis();
            if(stmt.execute()){
                rs = stmt.getResultSet();
                printData(rs);
            }else {
                UpdateCount =  stmt.getUpdateCount();
                if(UpdateCount != -1) {
                    System.out.println(UpdateCount + " row affected");
                }
            }
            System.out.println(System.currentTimeMillis() - time + "ms");
            while (true){
                if(stmt.getMoreResults()){
                    rs =  stmt.getResultSet();
                    printData(rs);
                }else{
                    UpdateCount =  stmt.getUpdateCount();
                    if(UpdateCount != -1) {
                        System.out.println(UpdateCount + "row affected");
                    }else{
                        break;
                    }
                }
            }
            if(rs != null) {
                rs.close();
            }
            rs = stmt.executeQuery("select * from t1");

            rs.last();

            System.out.println("this ==> " +rs.getRow());

            rs.moveToInsertRow();
            for(int i =1; i<= 4; ++i){
                rs.updateObject(i,entities.get(i-1));
            }
            time = System.currentTimeMillis();
            rs.insertRow();
            System.out.println(System.currentTimeMillis()-time+"ms");
            rs.last();

            System.out.println("that ==> " +rs.getRow());
            //printData(rs);
            if(rs != null) {
                rs.close();
            }
            stmt.close();
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestPreparedStatement end");
    }
}
