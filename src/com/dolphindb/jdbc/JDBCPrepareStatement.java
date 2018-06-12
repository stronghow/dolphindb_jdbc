package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;

public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {

    private String tableName;

    private Entity tableNameArg;

    private List<Entity> arguments;

    private BasicTable tableDFS;

    private boolean isInsert = false;

    private boolean isUpdate = false;

    private String tableType = null;

    private BasicString  colNames;

    private HashMap<Integer,Integer> colType;


    public JDBCPrepareStatement(JDBCConnection connection, String sql){
        super(connection);
        this.connection = connection;
        this.sql = sql.trim()+" ";
        if(this.sql.startsWith("insert")){
            tableName = sql.substring(sql.indexOf("into") + "into".length(), sql.indexOf("values"));
            isInsert = true;
        }else if(this.sql.startsWith("tableInsert")){
            tableName = sql.substring(sql.indexOf("(") + "(".length(), sql.indexOf(","));
            isInsert = true;
        }else if(sql.startsWith("append!")){
            tableName = sql.substring(sql.indexOf("(") + "(".length(), sql.indexOf(","));
            isInsert = true;
        }else if(sql.contains(".append!")){
            tableName = sql.split("\\.")[0];
            isInsert = true;
        }else if(this.sql.startsWith("update")){
            tableName = sql.substring(sql.indexOf("update") + "update".length(), sql.indexOf("set"));
            isUpdate = true;
        }else if(sql.contains(".update!")){
            tableName = sql.split("\\.")[0];
            isUpdate = true;
        }

        if(tableName != null){
            tableNameArg = new BasicString(tableName);
        }

        sqlSplit = this.sql.split("\\?");
        values = new Object[sqlSplit.length+1];
        arguments = new ArrayList<>(sqlSplit.length+1);
        batch = new StringBuilder();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(createSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
         return super.executeUpdate(createSql());
    }

    @Override
    public void setNull(int parameterIndex, int x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }


    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal bigDecimal) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,bigDecimal);
    }

    @Override
    public void setString(int parameterIndex, String s) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,s);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,bytes);
    }

    @Override
    public void setDate(int parameterIndex, Date date) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,date);
    }

    @Override
    public void setTime(int parameterIndex, Time time) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,time);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp timestamp) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,timestamp);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, int length) throws SQLException{

}

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length) throws SQLException{

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException{

    }

    @Override
    public void clearParameters() throws SQLException {
        super.clearBatch();
        for(Object item : values){
            item = null;
        }
    }



    @Override
    public void setObject(int parameterIndex, Object object) throws SQLException{
        super.checkClosed();
        values[parameterIndex] = object;
    }

    @Override
    public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException {
        setObject(parameterIndex,object);
    }

    @Override
    public void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex,object);
    }


    @Override
    public boolean execute() throws SQLException {
        super.checkClosed();
        try {
            if(isInsert){
                createArguments();
                if(tableType == null){
                    tableType = connection.getDbConnection().run("typestr " + tableName).getString();
                }
                BasicInt basicInt;

                if(tableType.equals(IN_MEMORY_TABLE)){
                    basicInt = (BasicInt) connection.getDbConnection().run("tableInsert",arguments);
                    objectQueue.offer(basicInt.getInt());
                }else{
                    int size = arguments.size();
                    if(size > 1) {
                        if(arguments.get(1) instanceof Vector) {
                            int insertRows = arguments.get(1).rows();
                            List<String> cols = new ArrayList<>();
                            List<Vector> entities = new ArrayList<>(size - 1);
                            for (int i = 1, len = size; i < len; i++) {
                                cols.add("" + i);
                                entities.add((Vector) arguments.get(i));
                            }
                            BasicTable table = new BasicTable(cols, entities);
                            List<Entity> newArguments = new ArrayList<>(2);
                            if(tableDFS == null){
                                tableDFS = (BasicTable) connection.getDbConnection().run(tableName);
                            }
                            newArguments.add(tableDFS);
                            newArguments.add(table);
                            connection.getDbConnection().run("append!", newArguments);
                            objectQueue.offer(insertRows);
                        }else{
                            int insertRows = 1;
                            List<String> cols = new ArrayList<>();
                            List<Vector> entities = new ArrayList<>(size - 1);
                            for (int i = 1, len = size; i < len; i++) {
                                cols.add("" + i);
                                BasicAnyVector basicAnyVector = new BasicAnyVector(1);
                                basicAnyVector.set(0,(Scalar) arguments.get(i));
                                entities.add(basicAnyVector);
                            }
                            BasicTable table = new BasicTable(cols, entities);
                            List<Entity> newArguments = new ArrayList<>(2);
                            if(tableDFS == null){
                                tableDFS = (BasicTable) connection.getDbConnection().run(tableName);
                            }
                            newArguments.add(tableDFS);
                            newArguments.add(table);
                            connection.getDbConnection().run("append!", newArguments);
                            objectQueue.offer(insertRows);
                        }
                    }
                }

                if(objectQueue.isEmpty()){
                    return false;
                }else {
                    result = objectQueue.poll();
                    if(result instanceof ResultSet){
                        return true;
                    }else{
                        return false;
                    }
                }
            }else if(isUpdate){
                if(tableType == null){
                    tableType = connection.getDbConnection().run("typestr " + tableName).getString();
                }
                if(tableType.equals(IN_MEMORY_TABLE)){
                    String s = createSql();
                    return super.execute(s);
                }else{
                    throw new SQLException("only local in-memory table can update");
//                    StringBuilder sb = new StringBuilder("update!(").append(tableName).append(",");
//                    String s = createSql();
//                    System.out.println(s);
//                    String substring = s.substring(s.indexOf("set") + "set".length(), s.indexOf("where"));
//                    Properties properties = new Properties();
//                    Utils.parseProperties(substring,properties,",","=");
//                    StringBuilder newValues = new StringBuilder("<[");
//                    for(String colName : properties.stringPropertyNames()){
//                        sb.append("\"").append(colName.trim()).append("\"");
//                        newValues.append(properties.getProperty(colName).trim()).append(",");
//                    }
//                    newValues.delete(newValues.length()-",".length(),newValues.length());
//                    newValues.append("]>");
//                    sb.append(",").append(newValues).append(",")
//                            .append("<")
//                            .append(s.substring(s.indexOf("where")+"where".length(),s.indexOf(";")))
//                            .append(">)");
//                    return super.execute(sb.toString());
                }
            }else{
                String s = createSql();
                return super.execute(s);
            }
        }catch (Exception e){
            throw new SQLException(e);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        super.checkClosed();
        batch.append(createSql()).append(";\n");

    }

    @Override
    public void clearBatch() throws SQLException {
        super.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return super.executeBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setRef(int parameterIndex, Ref ref) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setBlob(int parameterIndex, Blob blob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setClob(int parameterIndex, Clob clob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setArray(int parameterIndex, Array array) throws SQLException {
        Driver.unused();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return resultSet.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException{
        setObject(parameterIndex,date);
    }

    @Override
    public void setTime(int parameterIndex, Time time, Calendar cal) throws SQLException{
        setObject(parameterIndex,time);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp timestamp, Calendar cal) throws SQLException{
        setObject(parameterIndex,timestamp);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException{
        Driver.unused();
    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException{
        Driver.unused();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        Driver.unused();
        return  null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId rowId) throws SQLException{
        Driver.unused();
    }

    @Override
    public void setNString(int parameterIndex, String s) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setNClob(int parameterIndex, NClob nClob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML sqlxml) throws SQLException {
        Driver.unused();
    }


    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        Driver.unused();
    }


    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    public void setBasicDate(int parameterIndex, LocalDate date) throws SQLException{
        setObject(parameterIndex, date);
    }

    public void setBasicMonth(int parameterIndex, YearMonth yearMonth) throws SQLException{
        setObject(parameterIndex,new BasicMonth(yearMonth));
    }

    public void setBasicMinute(int parameterIndex, LocalTime time) throws SQLException{
        setObject(parameterIndex,new BasicMinute(time));
    }

    public void setBasicSecond(int parameterIndex, LocalTime time) throws SQLException{
        setObject(parameterIndex,new BasicSecond(time));
    }

    public void setBasicDateTime(int parameterIndex, LocalDateTime time) throws SQLException{
        setObject(parameterIndex,new BasicDateTime(time));
    }

    public void setBasicTimestamp(int parameterIndex, LocalDateTime time) throws SQLException{
        setObject(parameterIndex, new BasicTimestamp(time));
    }

    public void setBasicNanotime(int parameterIndex, LocalTime time) throws SQLException{
        setObject(parameterIndex, new BasicNanoTime(time));
    }

    public void setBasicNanotimestamp(int parameterIndex, LocalDateTime time) throws SQLException{
        setObject(parameterIndex, new BasicNanoTimestamp(time));
    }


    private void createArguments() throws Exception{
        if(colType == null){
            BasicDictionary dictionary = (BasicDictionary) connection.getDbConnection().run("schema(" + tableName + ")");
            BasicTable tableSchema =(BasicTable) dictionary.get(new BasicString("colDefs"));
            BasicIntVector typeInt = (BasicIntVector) tableSchema.getColumn("typeInt");
            int size = typeInt.rows();
            colType = new LinkedHashMap<>(size);
            for(int i=0; i<size; ++i){
                colType.put(i+1,typeInt.getInt(i));
            }
        }

        arguments.add(tableNameArg);
        for(int i = 1; i< sqlSplit.length; ++i){
            String s = TypeCast.TYPEINT2STRING.get(colType.get(i));
            System.out.println(s);
            arguments.add(TypeCast.java2db(values[i],s));
        }
    }



    private String createSql(){
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i< sqlSplit.length; ++i){
            String s = Utils.java2db(values[i]).toString();
            sb.append(sqlSplit[i-1]).append(s);
        }
        sb.append(sqlSplit[sqlSplit.length-1]);
        return sb.toString();
    }
}
