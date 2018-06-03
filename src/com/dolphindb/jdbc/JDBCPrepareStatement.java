package com.dolphindb.jdbc;

import com.xxdb.data.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.Calendar;

public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {

    public JDBCPrepareStatement(JDBCConnection conn, String sql){
        super(conn);
        this.connection = conn;
        this.sql = sql.trim();
        sqlSplit = this.sql.split("\\?");
        values = new Object[sqlSplit.length+1];
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
    public void setNull(int i, int i1) throws SQLException {
        super.checkClosed();
        setObject(i,i1);
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
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException{

}

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException{

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException{

    }

    @Override
    public void clearParameters() throws SQLException {
        super.clearBatch();
        for(Object item : values){
            item = null;
        }
    }



    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException{
        super.checkClosed();
        if(x instanceof Date){
            values[parameterIndex] = new BasicDate(LocalDate.parse(x.toString()));
        }else if(x instanceof Time){
            values[parameterIndex] = new BasicTime(LocalTime.parse(x.toString()));
        }else if(x instanceof Timestamp){
            values[parameterIndex] = new BasicTimestamp(LocalDateTime.parse(x.toString()));
        }else{
            values[parameterIndex] = x;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex,x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex,x);
    }


    @Override
    public boolean execute() throws SQLException {
        super.checkClosed();
        String s = createSql();
        return super.execute(s);
    }

    @Override
    public void addBatch() throws SQLException {
        super.checkClosed();
        batch.append(createSql()).append("\n");

    }

    @Override
    public void clearBatch() throws SQLException {
        super.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        //todo 需要修改
        return super.executeBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob blob) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob clob) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array array) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException{
        setObject(parameterIndex,x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException{
        setObject(parameterIndex,x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException{
        setObject(parameterIndex,x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException{

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException{

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException{

    }

    @Override
    public void setNString(int i, String s) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setNClob(int i, NClob nClob) throws SQLException {

    }

    @Override
    public void setClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setNClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {

    }


    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void setClob(int i, Reader reader) throws SQLException {

    }


    @Override
    public void setBlob(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int i, Reader reader) throws SQLException {

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





    private String createSql(){
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i< sqlSplit.length; ++i){
            sb.append(sqlSplit[i-1]).append(Utils.java2db(values[i]));
        }
        sb.append(sqlSplit[sqlSplit.length-1]);
        return sb.toString();
    }
}
