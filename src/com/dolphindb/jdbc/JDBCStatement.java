package com.dolphindb.jdbc;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Void;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.Queue;

public class JDBCStatement implements Statement {

    protected JDBCConnection connection;

    protected ResultSet resultSet;

    protected String sql;
    protected String[] sqlSplit;
    protected Object[] values;
    protected StringBuilder batch;
    protected Queue<Object> objectQueue;
    protected  Object result;

    protected boolean isClosed;


    public JDBCStatement(JDBCConnection cnn){
        this.connection = cnn;
        objectQueue = new LinkedList<>();
    }


    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        checkClosed();
        try {
            Entity entity = connection.getDbConnection().run(s);
            if(entity instanceof BasicTable){
                resultSet = new JDBCResultSet(connection, this, entity, s);
                return resultSet;
            }else{
                throw new SQLException("executeQuery can not create other than ResultSet");
            }
        }catch (IOException e){
            e.printStackTrace();
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        checkClosed();
        try {
             s = s.trim();
             if(s.startsWith("insert") || s.startsWith("tableInsert")){
                 return tableInsert(s).getInt();
             }else{
                Entity entity = connection.getDbConnection().run(s);
                if(entity instanceof Void){
                    return 0;
                }else if(entity instanceof BasicTable){
                    throw new SQLException("executeUpdate can not create ResultSet");
                }else{
                    // todo 等待 update delete api 返回更新行数
                    return 0;
                }
            }
        }catch (Exception e){
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        checkClosed();
        isClosed = true;
        sql = null;
        sqlSplit = null;
        values = null;
        batch = null;
        result = null;
        if(objectQueue != null){
            objectQueue.clear();
        }
        objectQueue = null;
        if(resultSet != null) {
            resultSet.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int i) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String s) throws SQLException {

    }

    @Override
    public boolean execute(String s) throws SQLException {
        checkClosed();
        try {
            String[] strings = s.split(";");
            System.out.println(strings.length);
            for(String item : strings){
                if(item.length()>0){
                    System.out.println(item);
                    if(item.startsWith("insert") || item.startsWith("tableInsert")){
                        objectQueue.offer(tableInsert(item).getInt());
                    }else if(item.startsWith("update")||item.startsWith("delete")){
                        //todo 获取更新计数
                        connection.getDbConnection().run(item);
                    }else{
                        Entity entity = connection.getDbConnection().run(item);
                        if(entity instanceof  BasicTable){
                            objectQueue.offer(new JDBCResultSet(connection,this,entity,item));
                        }
                    }
                }
            }
            if(objectQueue.isEmpty()){
                return false;
            }else {
                System.out.println(objectQueue.size());
                result = objectQueue.poll();
                if(result instanceof ResultSet){
                    return true;
                }else{
                    return false;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new SQLException("can not execute \n"+ s);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        if(result == null) {
            resultSet = null;
        }else if(result instanceof ResultSet){
            resultSet = (JDBCResultSet) result;
        }else{
            resultSet = null;
        }
        result = null;
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        int updateCount;
        if(result == null) {
            updateCount = -1;
        }else if(result instanceof Integer){
            updateCount =  (int)result;
        }else{
            updateCount = -1;
        }
        result = null;
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if(resultSet != null) {
            resultSet.close();
        }
        if(!objectQueue.isEmpty()){
            result = objectQueue.poll();
            if(result instanceof ResultSet){
                return true;
            }else{
                return false;
            }
        }else{
            return false;
        }
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int i) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String s) throws SQLException {
        checkClosed();
        batch.append(s).append("\n");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.delete(0,batch.length());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        try {
            connection.getDbConnection().run(batch.toString());
            return new int[0];
        }catch (Exception e){
            return new int[0];
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        return executeUpdate(s);
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        return executeUpdate(s);
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        return executeUpdate(s);
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        return execute(s);
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        return execute(s);
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        return execute(s);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;
    }

    protected BasicInt tableInsert(String s) throws Exception{
        if(s.startsWith("tableInsert")){
            return (BasicInt)connection.getDbConnection().run(s);
        }else {
            String tableName = s.substring(s.indexOf("into") + "into".length(), s.indexOf("values"));
            String values;
            int index = s.indexOf(";");
            if (index == -1) {
                values = s.substring(s.indexOf("values") + "values".length());
            } else {
                values = s.substring(s.indexOf("values") + "values".length(), s.indexOf(";"));
            }

            String sql = MessageFormat.format("tableInsert({0},{1})", tableName, values);
            BasicInt n = (BasicInt) connection.getDbConnection().run(sql);
            return n;
        }
    }

    protected void checkClosed() throws SQLException{
        if(isClosed()){
            throw new SQLException("Statement is closed");
        }
        if(connection == null||connection.isClosed()){
            throw new SQLException("Connection is closed");
        }
    }
}
