package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.*;

/**
 * JDBCResultSet Operation is in memory only, not persisted
 */

public class JDBCResultSet implements ResultSet{
    private JDBCConnection conn;
    private JDBCStatement statement;
    private BasicTable table;
    private int row = -1;
    private int rows;
    private String tableName;
    private Entity tableNameArg;
    private List<Entity> arguments;

    private HashMap<String,Integer> findColumnHashMap;

    private int updateRow;

    private int insertRow;

    private HashMap<Integer,Entity> insertRowMap;

    private boolean isInsert;

    private boolean isClosed = false;

    private boolean isUpdateable = true;


    public JDBCResultSet(JDBCConnection conn, JDBCStatement statement, Entity entity, String sql) throws SQLException{
        sql = sql.replaceAll("\n","").trim();
        this.conn = conn;
        this.statement = statement;

        if(entity.isTable()){
            this.table = (BasicTable) entity;
        }else{
            throw new SQLException("ResultSet data is null");
        }
        rows = this.table.rows();

        findColumnHashMap = new HashMap<>(this.table.columns());

        insertRowMap = new HashMap<>(this.table.columns()+1);

        for(int i=0; i<this.table.columns(); ++i){
            findColumnHashMap.put(this.table.getColumnName(i),i+1);
        }

        if(sql == null || sql.length() == 0){
            this.isUpdateable = false;
        }else{
            this.isUpdateable = Utils.isUpdateable(sql);

            if(this.isUpdateable){
                this.tableName = Utils.getTableName(sql);
                if(Utils.isUpdateable(this.tableName)){
                    String s = run("typestr " + tableName).getString();
                    if(!s.equals("IN-MEMORY TABLE")){
                        this.isUpdateable = false;
                    }else{
                        tableNameArg = new BasicString(tableName);
                    }
                }
            }else{
                this.tableName = "";
            }
        }

        System.out.println(table.rows() + "  "+ table.columns());
    }


    private BasicTable loadTable() throws SQLException{
        return (BasicTable) run(tableName);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        row++;
        return row <= rows-1;
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
        findColumnHashMap = null;
        insertRowMap = null;
        table = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException{
        return ((Entity)getObject(columnIndex)).getString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException{
        return ((BasicBoolean)getObject(columnIndex)).getBoolean();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException{
        return ((BasicByte)getObject(columnIndex)).getByte();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return ((BasicShort)getObject(columnIndex)).getShort();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return ((BasicInt)getObject(columnIndex)).getInt();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return ((BasicLong)getObject(columnIndex)).getLong();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return ((BasicFloat)getObject(columnIndex)).getFloat();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return ((BasicDouble)getObject(columnIndex)).getDouble();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException{
        Driver.unused();
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return  toByteArray(getObject(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        Scalar scalar = (Scalar) getObject(columnIndex);
        LocalDate date = null;
        if(scalar instanceof BasicDate){
            date = ((BasicDate) scalar).getDate();
        }
        if (date==null) return null;
        return new Date(date.getYear(),date.getMonthValue(),date.getDayOfMonth());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        Scalar scalar = (Scalar) getObject(columnIndex);
        LocalTime time = null;
        if(scalar instanceof BasicMinute){
            time = ((BasicMinute) scalar).getMinute();
        }else if(scalar instanceof BasicSecond){
            time = ((BasicSecond) scalar).getSecond();
        }else if(scalar instanceof BasicNanoTime){
            time = ((BasicNanoTime) scalar).getNanoTime();
        }
        if (time==null) return null;
        return new Time(time.getHour(),time.getMinute(),time.getSecond());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        Scalar scalar = (Scalar) getObject(columnIndex);
        LocalDateTime dateTime = null;
        if(scalar instanceof BasicDateTime){
            dateTime = ((BasicDateTime) scalar).getDateTime();
        }else if(scalar instanceof BasicTimestamp){
            dateTime = ((BasicTimestamp) scalar).getTimestamp();
        }else if(scalar instanceof BasicNanoTimestamp){
            dateTime = ((BasicNanoTimestamp) scalar).getNanoTimestamp();
        }
        if (dateTime==null) return null;
        return new Timestamp(dateTime.getYear(),dateTime.getMonthValue(),dateTime.getDayOfMonth(),
                dateTime.getHour(), dateTime.getMinute(),dateTime.getSecond(),dateTime.getNano());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException{
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException{
        Driver.unused();
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        Driver.unused();
    }

    @Override
    public String getCursorName() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new JDBCResultSetMetaData(table);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        return table.getColumn(adjustColumnIndex(columnIndex)).get(row);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkClosed();
        return table.getColumn(columnLabel).get(row);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        return findColumnHashMap.get(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return row < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return row >= rows;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return row == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return row == rows-1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        row = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        row = rows;
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        row =0;
        return rows > 0;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        row = rows -1;
        return  rows > 0;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return row+1;
    }

    @Override
    public boolean absolute(int columnIndex) throws SQLException {
        checkClosed();
        row = columnIndex-1;
        return row < rows;
    }

    @Override
    public boolean relative(int columnIndex) throws SQLException {
        checkClosed();
        row += columnIndex;
        return  row >= 0 && row < rows;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        --row;
        return row>=0;
    }

    @Override
    public void setFetchDirection(int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getFetchSize() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        Driver.unused();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        Driver.unused();
        return  false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        Driver.unused();
        return  false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal bigDecimal) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] bytes) throws SQLException {
        update(columnIndex,toObject(bytes));
    }

    @Override
    public void updateDate(int columnIndex, Date date) throws SQLException {
        update(columnIndex,date);
    }

    @Override
    public void updateTime(int columnIndex, Time time) throws SQLException {
        update(columnIndex,time);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp timestamp) throws SQLException {
        update(columnIndex,timestamp);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream inputStream, int columnIndex1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream, int columnIndex1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, int columnIndex1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateObject(int columnIndex, Object o, int columnIndex1) throws SQLException {
        update(columnIndex,o);
    }

    @Override
    public void updateObject(int columnIndex, Object o) throws SQLException {
        update(columnIndex,o);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal bigDecimal) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] bytes) throws SQLException {
        update(columnLabel,bytes);
    }

    @Override
    public void updateDate(String columnLabel, Date date) throws SQLException {
        update(columnLabel,date);
    }

    @Override
    public void updateTime(String columnLabel, Time time) throws SQLException {
        update(columnLabel,time);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp timestamp) throws SQLException {
        update(columnLabel,timestamp);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream inputStream, int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream inputStream, int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateObject(String columnLabel, Object o, int columnIndex) throws SQLException {
        update(columnLabel,o);
        
    }

    @Override
    public void updateObject(String columnLabel, Object o) throws SQLException {
        update(columnLabel,o);
        
    }

    @Override
    public void insertRow() throws SQLException {
        isUpdateable();
        try {
            if(insertRow == row){
                createArguments();
                conn.getDbConnection().run("tableInsert",arguments);
                //insertRun();
                table = loadTable();
                rows = table.rows();
            }
            arguments.clear();
            insertRowMap.clear();
            isInsert = false;
        }catch (Exception e){
            throw new SQLException(e);
        }

    }

    @Override
    public void updateRow() throws SQLException {
        isUpdateable();
        if(updateRow == row){
            updateRun();
//            for(int index : insertRowMap.keySet()){
//                update1(index,insertRowMap.get(index));
//            }
            table = loadTable();
            rows = table.rows();
        }
        insertRowMap.clear();
    }

    @Override
    public void deleteRow() throws SQLException {
        isUpdateable();
        StringBuilder sb = new StringBuilder("delete from ").append(tableName).append(" where ");
        for(int i=1; i<=table.columns(); ++i){
            sb.append(getColumnName(i)).append(" = ").append(Utils.java2db(getObject(i))).append(", ");
        }
        sb.delete(sb.length()-2,sb.length());
        String sql = sb.toString();
        run(sql);
        table = loadTable();
        rows = table.rows();
    }

    @Override
    public void refreshRow() throws SQLException {
        checkClosed();
        isUpdateable();
        BasicTable newTable = loadTable();
        try {
            for(int i=0; i<newTable.columns(); ++i){
                table.getColumn(i).set(row,newTable.getColumn(i).get(row));
            }
        }catch (Exception e){
            throw new SQLException(e);
        }

    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        checkClosed();
        isUpdateable();
        if(isInsert){
            throw new SQLException("cursor is on the insert row");
        }
        insertRowMap.clear();
        isInsert = false;
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        checkClosed();
        isUpdateable();
        isInsert = true;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        checkClosed();
        isInsert = false;
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(adjustColumnIndex(columnIndex));
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException{
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        Driver.unused();
        return getDate(adjustColumnIndex(columnIndex));
    }

    @Override
    public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
        Driver.unused();
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        Driver.unused();
        return getTime(adjustColumnIndex(columnIndex));
    }

    @Override
    public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
        Driver.unused();
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        Driver.unused();
        return getTimestamp(adjustColumnIndex(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
        Driver.unused();
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref ref) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateRef(String columnLabel, Ref ref) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBlob(int columnIndex, Blob blob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBlob(String columnLabel, Blob blob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateClob(int columnIndex, Clob clob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateClob(String columnLabel, Clob clob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateArray(int columnIndex, Array array) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateArray(String columnLabel, Array array) throws SQLException {
        Driver.unused();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId rowId) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateRowId(String columnLabel, RowId rowId) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getHoldability() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void updateNString(int columnIndex, String columnLabel) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNString(String columnLabel, String columnLabel1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        Driver.unused();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML sqlxml) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML sqlxml) throws SQLException {
        Driver.unused();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long l) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        Driver.unused();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> aClass) throws SQLException {
        checkClosed();
        return (T)getObject(columnIndex);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> aClass) throws SQLException {
        checkClosed();
        return (T)getObject(columnLabel);
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        checkClosed();
        return aClass.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        checkClosed();
        return aClass.isInstance(this);
    }

    public BasicDate getBasicDate(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicDate)getObject(columnLabel);
    }

    public BasicMonth getBasicMonth(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicMonth)getObject(columnLabel);
    }

    public BasicTime getBasicTime(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicTime)getObject(columnLabel);
    }

    public BasicMinute getBasicMinute(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicMinute)getObject(columnLabel);
    }

    public BasicSecond getBasicSecond(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicSecond)getObject(columnLabel);
    }

    public BasicDateTime getBasicDateTime(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicDateTime)getObject(columnLabel);
    }

    public BasicNanoTime getBasicNanoTime(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicNanoTime)getObject(columnLabel);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(String columnLabel) throws SQLException{
        checkClosed();
        return (BasicNanoTimestamp)getObject(columnLabel);
    }

    public BasicDate getBasicDate(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicDate)getObject(columnIndex);
    }

    public BasicMonth getBasicMonth(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicMonth)getObject(columnIndex);
    }

    public BasicTime getBasicTime(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicTime)getObject(columnIndex);
    }

    public BasicMinute getBasicMinute(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicMinute)getObject(columnIndex);
    }

    public BasicSecond getBasicSecond(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicSecond)getObject(columnIndex);
    }

    public BasicDateTime getBasicDateTime(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicDateTime)getObject(columnIndex);
    }

    public BasicNanoTime getBasicNanoTime(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicNanoTime)getObject(columnIndex);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(int columnIndex) throws SQLException{
        checkClosed();
        return (BasicNanoTimestamp)getObject(columnIndex);
    }

    private void update(String name, Object value) throws SQLException{
        update(findColumn(name),value);
    }



    private void update(int columnIndex, Object value) throws SQLException{
        checkClosed();
        isUpdateable();
        if(isInsert){
            insertRow = row;
        }else{
            updateRow = row;
        }
        insert(columnIndex, value);
    }

    private void insert(String name, Object value) throws SQLException{
        insert(findColumn(name),value);
    }


    private void insert(int columnIndex, Object value) throws SQLException {
        checkClosed();
        if (!insertDateTime(columnIndex, value)) {
            if(!insertBasicType(columnIndex,value)) {
                if (value instanceof Scalar) {
                    insertRowMap.put(columnIndex, (Scalar) value);
                } else if (value instanceof Vector) {
                    insertRowMap.put(columnIndex, (Vector) value);
                } else {
                    throw new SQLException("only supports LocalDate  LocalTime  LocalDateTime YearMonth  Date Time Timestamp boolean int char short long float double string scalar vector");
                }
            }
        }
    }

    private boolean insertBasicType(String name, Object value) throws SQLException{
        return insertDateTime(findColumn(name),value);
    }

    private boolean insertBasicType(int columnIndex, Object srcValue) throws SQLException{
        Vector targetVector = table.getColumn(adjustColumnIndex(columnIndex));
        Scalar targetScalar = targetVector.get(row);
        try {
            String s = null;
            if(srcValue instanceof Scalar){
                if(srcValue.getClass().getName().equals(targetScalar.getClass().getName())){
                    insertRowMap.put(columnIndex,(Scalar)srcValue);
                    return true;
                }
                if(srcValue instanceof BasicBoolean ||
                        srcValue instanceof BasicByte ||
                        srcValue instanceof BasicInt ||
                        srcValue instanceof  BasicShort ||
                        srcValue instanceof BasicLong ||
                        srcValue instanceof BasicFloat ||
                        srcValue instanceof BasicDouble){


                    if(targetScalar instanceof BasicBoolean ||
                            targetScalar instanceof BasicByte ||
                            targetScalar instanceof BasicInt ||
                            targetScalar instanceof  BasicShort ||
                            targetScalar instanceof BasicLong ||
                            targetScalar instanceof BasicFloat ||
                            targetScalar instanceof BasicDouble ||
                            targetScalar instanceof BasicString){
                        insertRowMap.put(columnIndex,(Scalar)srcValue);
                        return true;
                    }else{
                        throw new SQLException(srcValue.getClass().getName() + "can not cast " + targetScalar.getClass().getName());
                    }
                }else if(srcValue instanceof BasicString){

                    if(targetScalar instanceof BasicString){
                        insertRowMap.put(columnIndex,(Scalar)srcValue);
                        return true;
                    }else{
                        throw new SQLException(srcValue.getClass().getName() + "can not cast " + targetScalar.getClass().getName());
                    }
                }else{

                    throw new SQLException(srcValue.getClass().getName() + "can not cast " + targetScalar.getClass().getName());
                }
            }else if(srcValue instanceof Vector){
                int size = ((Vector) srcValue).rows();
                if(size == 0) throw  new SQLException("Vector size can not 0");
                if(srcValue.getClass().getName().equals(targetVector.getClass().getName())){
                    insertRowMap.put(columnIndex,(Vector)srcValue);
                    return true;
                }

                Scalar srcValue_ = ((Vector) srcValue).get(0);

                if(srcValue_ instanceof BasicBoolean ||
                        srcValue_ instanceof BasicByte ||
                        srcValue_ instanceof BasicInt ||
                        srcValue_ instanceof  BasicShort ||
                        srcValue_ instanceof BasicLong ||
                        srcValue_ instanceof BasicFloat ||
                        srcValue_ instanceof BasicDouble){


                    if(targetScalar instanceof BasicBoolean ||
                            targetScalar instanceof BasicByte ||
                            targetScalar instanceof BasicInt ||
                            targetScalar instanceof  BasicShort ||
                            targetScalar instanceof BasicLong ||
                            targetScalar instanceof BasicFloat ||
                            targetScalar instanceof BasicDouble ||
                            targetScalar instanceof BasicString){
                        insertRowMap.put(columnIndex,(Vector)srcValue);
                        return true;
                    }else{
                        throw new SQLException(srcValue_.getClass().getName() + "can not cast " + targetScalar.getClass().getName());
                    }
                }else if(srcValue_ instanceof BasicString){

                    if(targetScalar instanceof BasicString){
                        insertRowMap.put(columnIndex,(Vector)srcValue);
                        return true;
                    }else{
                        throw new SQLException(srcValue_.getClass().getName() + "can not cast " + targetScalar.getClass().getName());
                    }
                }else{
                    throw new SQLException(srcValue.getClass().getName() + "can not cast " + targetScalar.getClass().getName());
                }

            }else if(srcValue instanceof List){
                int size = ((List) srcValue).size();
                if(size == 0) throw new SQLException("List size can not 0");
                Object o = ((List) srcValue).get(0);
                List value_ = (List) srcValue;
                List<String> strings = null;
                if(o instanceof Boolean){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Boolean) value_.get(i)).toString());
                    }
                }else if(o instanceof Byte){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Byte) value_.get(i)).toString());
                    }
                }else if(o instanceof Character){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Character) value_.get(i)).toString());
                    }
                }else if(o instanceof Short){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Short) value_.get(i)).toString());
                    }
                }else if(o instanceof Integer){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Integer) value_.get(i)).toString());
                    }
                }else if(o instanceof Long){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Long) value_.get(i)).toString());
                    }
                }else if(o instanceof Float){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Float) value_.get(i)).toString());
                    }
                }else if(o instanceof Double){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Double) value_.get(i)).toString());
                    }
                }else if(o instanceof String){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((String) value_.get(i)));
                    }
                }else if(o instanceof Scalar){
                    throw new SQLException("you need use com.xxdb.data.Vector load com.xxdb.data.Scalar");
                }
                if(strings == null){
                    return false;
                }else{
                    insertRowMap.put(columnIndex,basicTypeVectorCast(strings,targetVector));
                    return true;
                }
            }else if(srcValue instanceof Object[]){
                int size = ((Object[]) srcValue).length;
                if(size == 0) throw new SQLException("Object[] size can not 0");
                Object o = ((Object[]) srcValue)[0];
                Object[] value_ = (Object[]) srcValue;
                List<String> strings = null;
                if(o instanceof Boolean){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add((((Boolean) value_[i])).toString());
                    }
                }else if(o instanceof Byte){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add((((Byte) value_[i])).toString());
                    }
                }else if(o instanceof Character){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Character) value_[i]).toString());
                    }
                }else if(o instanceof Short){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Short) value_[i]).toString());
                    }
                }else if(o instanceof Integer){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Integer) value_[i]).toString());
                    }
                }else if(o instanceof Long){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Long) value_[i]).toString());
                    }
                }else if(o instanceof Float){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Float) value_[i]).toString());
                    }
                }else if(o instanceof Double){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((Double) value_[i]).toString());
                    }
                }else if(o instanceof String){
                    strings = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        strings.add(((String) value_[i]));
                    }
                }else if(o instanceof Scalar){
                    throw new SQLException("you need use com.xxdb.data.Vector load com.xxdb.data.Scalar");
                }
                if(strings == null){
                    return false;
                }else{
                    insertRowMap.put(columnIndex,basicTypeVectorCast(strings,targetVector));
                    return true;
                }
            }else if(srcValue instanceof Boolean || srcValue instanceof Byte || srcValue instanceof Character ||
                    srcValue instanceof Short || srcValue instanceof Integer || srcValue instanceof Long ||
                    srcValue instanceof Float || srcValue instanceof Double || srcValue instanceof String){
                s = srcValue.toString();
            }

            if(s==null){
                return false;
            }else {
                insertRowMap.put(columnIndex, basicTypeCast(s, targetVector));
                return true;
            }
        }catch (Exception e){
            throw new SQLException(e);
        }
    }



    private boolean insertDateTime(String name, Object value) throws SQLException{
        return insertDateTime(findColumn(name),value);
    }

    private boolean insertDateTime(int columnIndex, Object value) throws SQLException{
        if(value == null) throw new SQLException("can not insert null");
        Entity entity = table.getColumn(adjustColumnIndex(columnIndex));
        String srcClassName = value.getClass().getName();
        try {
            Temporal temporal=null;
            if(value instanceof Scalar){
                Scalar scalar = ((Vector) entity).get(row);
                if(srcClassName.equals(scalar.getClass().getName())){
                    insertRowMap.put(columnIndex, scalar);
                    return true;
                }
                temporal = TypeCast.getTemporal(scalar);
                if(temporal == null){
                    return false;
                }else{
                    insertRowMap.put(columnIndex, TypeCast.DataTimeCast(temporal, srcClassName, scalar));
                    return true;
                }
            }else if(value instanceof Vector){
                int size = ((Vector) value).rows();
                if(size == 0) throw  new SQLException("Vector size can not 0");
                List<Temporal> temporals = null;
                if(srcClassName.equals(entity.getClass().getName())){
                    insertRowMap.put(columnIndex, entity);
                    return true;
                }else if(value instanceof BasicMonthVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicMonthVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicDateVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicDateVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicTimeVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicTimeVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicMinuteVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicMinuteVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicSecondVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicSecondVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicNanoTimeVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicNanoTimeVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicDateTimeVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicDateTimeVector) value).get(i)).getTemporal());
                    }
                }else if(value instanceof BasicNanoTimestampVector){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add((((BasicNanoTimestampVector) value).get(i)).getTemporal());
                    }
                }

                if(temporals == null){
                    return false;
                }else{
                    insertRowMap.put(columnIndex, DataTimeVectorCast(temporals, srcClassName, entity));
                    return true;
                }

            }else if(value instanceof List){
                int size = ((List) value).size();
                if(size == 0) throw new SQLException("List size can not 0");
                Object o = ((List) value).get(0);
                List value_ = (List) value;
                List<Temporal> temporals = null;
                if(o instanceof Date){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicDate(((Date) value_.get(i)).toLocalDate()).getTemporal());
                    }
                }else if(o instanceof Time){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTime(((Time) value_.get(i)).toLocalTime()).getTemporal());
                    }
                }else if(o instanceof Timestamp){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTimestamp(((Timestamp) value_.get(i)).toLocalDateTime()).getTemporal());
                    }
                }else if(o instanceof LocalDate){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicDate(((LocalDate) value_.get(i))).getTemporal());
                    }
                }else if(o instanceof LocalTime){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTime((LocalTime) value_.get(i)).getTemporal());
                    }
                }else if(o instanceof LocalDateTime){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTimestamp((LocalDateTime) value_.get(i)).getTemporal());
                    }
                }else if(o instanceof Scalar){
                    throw new SQLException("you need use com.xxdb.data.Vector");
                }

                if(temporals == null){
                    return false;
                }else{
                    insertRowMap.put(columnIndex, DataTimeVectorCast(temporals, srcClassName, entity));
                    return true;
                }

            }else if(value instanceof Object[]){
                int size = ((Object[]) value).length;
                if(size == 0) throw new SQLException("Object[] size can not 0");
                Object o = ((Object[]) value)[0];
                Object[] value_ = (Object[]) value;
                List<Temporal> temporals = null;
                if(o instanceof Date){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicDate(((Date) value_[i]).toLocalDate()).getTemporal());
                    }
                }else if(o instanceof Time){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTime(((Time) value_[i]).toLocalTime()).getTemporal());
                    }
                }else if(o instanceof Timestamp){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTimestamp(((Timestamp) value_[i]).toLocalDateTime()).getTemporal());
                    }
                }else if(o instanceof LocalDate){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicDate(((LocalDate) value_[i])).getTemporal());
                    }
                }else if(o instanceof LocalTime){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTime((LocalTime) value_[i]).getTemporal());
                    }
                }else if(o instanceof LocalDateTime){
                    temporals = new ArrayList<>(size);
                    for(int i=0; i<size; ++i){
                        temporals.add(new BasicNanoTimestamp((LocalDateTime) value_[i]).getTemporal());
                    }
                }else if(o instanceof Scalar){
                    throw new SQLException("you need use com.xxdb.data.Vector");
                }

                if(temporals == null){
                    return false;
                }else{
                    insertRowMap.put(columnIndex, DataTimeVectorCast(temporals, srcClassName, entity));
                    return true;
                }
            }else {
                temporal = TypeCast.getTemporal(value);
            }

            if(temporal==null){
                return false;
            }else {
                insertRowMap.put(columnIndex, TypeCast.DataTimeCast(temporal, srcClassName, entity));
                return true;
            }
        }catch (Exception e){
            throw new SQLException(e);
        }

    }

    private Entity basicTypeCast(String s,Entity entity) throws SQLException{
        if(entity instanceof BasicBooleanVector){
            return new BasicBoolean(Boolean.parseBoolean(s));
        }else if(entity instanceof BasicByteVector){
            return new BasicByte(Byte.parseByte(s));
        }else if(entity instanceof BasicIntVector){
            return new BasicInt(Integer.valueOf(s));
        }else if(entity instanceof BasicShortVector){
            return new BasicShort(Short.valueOf(s));
        }else if(entity instanceof BasicLongVector){
            return new BasicLong(Long.valueOf(s));
        }else if(entity instanceof BasicFloatVector){
            return new BasicFloat(Float.valueOf(s));
        }else if(entity instanceof BasicDoubleVector){
            return new BasicDouble(Double.valueOf(s));
        }else if(entity instanceof BasicStringVector){
            return new BasicString(s);
        }else{
            throw new SQLException("can not cast basicType");
        }
    }

    private Entity basicTypeVectorCast(List<String> strings,Entity entity) throws SQLException{
        try {
            Vector vector;
            int size = strings.size();
            if(entity instanceof BasicBooleanVector){
                vector = new BasicBooleanVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index, new BasicBoolean(Boolean.parseBoolean(s)));
                    ++index;
                }
            }else if(entity instanceof BasicByteVector){
                vector = new BasicByteVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicByte(Byte.parseByte(s)));
                    ++index;
                }
            }else if(entity instanceof BasicIntVector){
                vector = new BasicIntVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicInt(Integer.valueOf(s)));
                    ++index;
                }
            }else if(entity instanceof BasicShortVector){
                vector = new BasicShortVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicShort(Short.valueOf(s)));
                    ++index;
                }
            }else if(entity instanceof BasicLongVector){
                vector = new BasicLongVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicInt(Integer.valueOf(s)));
                    ++index;
                }
            }else if(entity instanceof BasicFloatVector){
                vector = new BasicFloatVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicFloat(Float.valueOf(s)));
                    ++index;
                }
            }else if(entity instanceof BasicDoubleVector){
                vector = new BasicDoubleVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicDouble(Double.valueOf(s)));
                    ++index;
                }
            }else if(entity instanceof BasicStringVector){
                vector = new BasicStringVector(size);
                int index = 0;
                for (String s : strings) {
                    vector.set(index,  new BasicString(s));
                    ++index;
                }
            }else{
                throw new SQLException("can not cast basicType");
            }
            return  vector;
        }catch (Exception e){
            throw new SQLException("can not cast basicType");
        }

    }


    private Entity DataTimeCast(Temporal temporal, Entity srcEntity, Entity targetEntity) throws SQLException{
        if(srcEntity.getClass().getName().equals(targetEntity.getClass().getName()))
            return srcEntity;

        if(targetEntity instanceof  BasicMonthVector){
            return new BasicMonth(YearMonth.from(temporal));
        }else if(targetEntity instanceof BasicDateVector){
            return new BasicDate(LocalDate.from(temporal));
        }else if(targetEntity instanceof BasicTimeVector){
            return new BasicTime(LocalTime.from(temporal));
        }else if(targetEntity instanceof BasicMinuteVector){
            return new BasicMinute(LocalTime.from(temporal));
        }else if(targetEntity instanceof BasicSecondVector){
            return new BasicSecond(LocalTime.from(temporal));
        }else if(targetEntity instanceof BasicNanoTimeVector){
            return new BasicNanoTime(LocalTime.from(temporal));
        }else if(targetEntity instanceof BasicTimestampVector){
            return new BasicTimestamp(LocalDateTime.from(temporal));
        }else if(targetEntity instanceof BasicDateTimeVector){
            return new BasicDateTime(LocalDateTime.from(temporal));
        }else if(targetEntity instanceof BasicNanoTimestampVector){
            return new BasicNanoTimestamp(LocalDateTime.from(temporal));
        }else{
            throw new SQLException("can not cast DataTime");
        }
    }

    private Entity DataTimeVectorCast(List<Temporal> temporals, String srcClassName, Entity tagetEntity) throws SQLException{
        try {
            Vector vector = null;
            int size = temporals.size();
            if(tagetEntity instanceof  BasicMonthVector){
                vector = new BasicMonthVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicMonth(YearMonth.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicDateVector){
                vector = new BasicDateVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicDate(LocalDate.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicTimeVector){
                vector = new BasicTimeVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicTime(LocalTime.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicMinuteVector){
                vector = new BasicTimeVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicMinute(LocalTime.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicSecondVector){
                vector = new BasicSecondVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicSecond(LocalTime.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicNanoTimeVector){
                vector = new BasicNanoTimeVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicNanoTime(LocalTime.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicTimestampVector){
                vector = new BasicNanoTimeVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicTimestamp(LocalDateTime.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicDateTimeVector){
                vector = new BasicDateTimeVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicDateTime(LocalDateTime.from(temporal)));
                    ++index;
                }
            }else if(tagetEntity instanceof BasicNanoTimestampVector){
                vector = new BasicNanoTimestampVector(size);
                int index = 0;
                for(Temporal temporal : temporals){
                    vector.set(index,new BasicNanoTimestamp(LocalDateTime.from(temporal)));
                    ++index;
                }
            }
            if(vector == null){
                throw new SQLException(srcClassName.getClass().getName() + " can not cast DataTime");
            }else{
                return vector;
            }
        }catch (Exception e){
            throw new SQLException(srcClassName.getClass().getName() + " can not cast DataTime");
        }
    }

    
    private void updateRun() throws SQLException{
        Entity value;
        StringBuilder sb = new StringBuilder();
        StringBuilder where = new StringBuilder(" where ");
        sb.append("update ").append(tableName).append(" set ");
        for (int i = 1; i <= table.columns(); ++i) {
            if((value = insertRowMap.get(i)) != null){
                sb.append(getColumnName(i)).append(" = ").append(Utils.java2db(value)).append(", ");
            }
            where.append(getColumnName(i)).append(" = ").append(Utils.java2db(table.getColumn(adjustColumnIndex(i)).get(row))).append(" ,");
        }
        sb.delete(sb.length()-2,sb.length());
        where.delete(where.length()-2,where.length());
        sb.append(where);

        String sql = sb.toString();
        run(sql);
    }


    private void insertRun() throws SQLException{
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName).append(" values ( ");
        for (int i = 1; i <= table.columns(); ++i) {
            sb.append(Utils.java2db(insertRowMap.get(i))).append(", ");

        }
        sb.delete(sb.length()-2,sb.length());
        sb.append(")");
        String sql = sb.toString();
        run(sql);
    }

    private Entity run(String sql) throws SQLException {
        try {
            return conn.getDbConnection().run(sql);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private String JAVA2DBString(Entity entity, Object value){
        if(value instanceof Boolean){
            return new BasicBoolean((boolean) value).getString();
        }else if(value instanceof Byte){
            return new BasicByte((byte) value).getString();
        }else if(value instanceof Integer){
            return new BasicInt((int) value).getString();
        }else if(value instanceof Short){
            return new BasicShort((short) value).getString();
        }else if(value instanceof Long){
            return new BasicLong((long) value).getString();
        }else if(value instanceof Float){
            return new BasicFloat((float) value).getString();
        }else if(value instanceof Double){
            return new BasicDouble((double) value).getString();
        }else if(value instanceof String){
            return "`" + new BasicString((String) value).getString();
        }else if(value instanceof Date){
            return JAVA2DBString4DataTime(entity,((Date) value).toLocalDate());
        }else if(value instanceof Time){
            return JAVA2DBString4DataTime(entity,((Time) value).toLocalTime());
        }else if(value instanceof Timestamp){
            return JAVA2DBString4DataTime(entity,((Timestamp) value).toLocalDateTime());
        }else{
            return JAVA2DBString4DataTime(entity,value);
        }
    }

    private String JAVA2DBString4DataTime(Entity entity, Object value){
        if(value instanceof LocalDate){
            if(entity instanceof BasicDate){
                return new BasicDate((LocalDate) value).getString();
            }
        }else if(value instanceof LocalTime){
            if(entity instanceof BasicTime){
                return new BasicTime((LocalTime) value).getString();
            } else if(entity instanceof BasicMinuteVector){
                return new BasicMinute((LocalTime) value).getString();
            }else if(entity instanceof BasicSecondVector){
                return new BasicSecond((LocalTime) value).getString();
            }else if(entity instanceof BasicNanoTime){
                return new BasicNanoTime((LocalTime) value).getString();
            }
        }else if(value instanceof LocalDateTime){
            if(entity instanceof BasicTimestamp){
                return new BasicTimestamp((LocalDateTime) value).getString();
            }else if(entity instanceof BasicDateTime){
                return new BasicDateTime((LocalDateTime) value).getString();
            }else if(entity instanceof BasicNanoTimestampVector){
                return new BasicNanoTimestamp((LocalDateTime) value).getString();
            }
        }else if(value instanceof YearMonth){
            if(entity instanceof BasicMonth){
                return new BasicMonth((YearMonth) value).getString();
            }
        }

        return "";
    }

    private String getColumnName(int columnIndex){
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }

    private int adjustColumnIndex(int columnIndex){
        return columnIndex-1;
    }

    private void checkClosed() throws SQLException{
        if(table == null && isClosed){
            throw new SQLException("ResultSet is closed");
        }
    }

    public byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    public Object toObject (byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }

     public void isUpdateable() throws SQLException{
         if(!isUpdateable) throw new SQLException("Unable to update join table");
     }

    private void createArguments(){
        int col = table.columns();
        if(arguments == null){
            arguments = new ArrayList<>(col+1);
        }
        arguments.add(tableNameArg);
        for(int i=1; i<= col; ++i){
            arguments.add(insertRowMap.get(i));
        }
    }

}
