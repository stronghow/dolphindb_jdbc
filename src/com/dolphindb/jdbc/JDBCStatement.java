package com.dolphindb.jdbc;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Void;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

public class JDBCStatement implements Statement {

    protected JDBCConnection connection;

    protected ResultSet resultSet;

    protected String sql;
    protected String[] sqlSplit;
    protected Object[] values;
    protected StringBuilder batch;
    protected Queue<Object> objectQueue;
    protected Object result;
    protected Deque<ResultSet> resultSets;

    protected static final String IN_MEMORY_TABLE = "IN-MEMORY TABLE";

    protected boolean isClosed;


    public JDBCStatement(JDBCConnection cnn){
        this.connection = cnn;
        objectQueue = new LinkedList<>();
        resultSets = new LinkedList<>();
    }


    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        try {
            Entity entity = connection.run(sql);
            if(entity instanceof BasicTable){
                resultSet = new JDBCResultSet(connection, this, entity, sql);
                return resultSet;
            }else{
                throw new SQLException("the given SQL statement produces anything other than a single ResultSet object");
            }
        }catch (IOException e){
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        try {
            sql = sql.trim();
            if(sql.startsWith("tableInsert")){
                return tableInsert(sql).getInt();
            }else{
                Entity entity = connection.run(sql);
                if(entity instanceof Void){
                    return 0;
                }else if(entity instanceof BasicTable){
                    throw new SQLException("the given SQL statement produces a ResultSet object");
                }else{
                    // todo  update delete api return row
                    return 0;
                }
            }
        }catch (Exception e){
            throw new SQLException(e.getMessage());
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
        Driver.unused();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int maxFieldSize) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getMaxRows() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setQueryTimeout(int queryTimeout) throws SQLException {
        Driver.unused();
    }

    @Override
    public void cancel() throws SQLException {
        Driver.unused();
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
    public void setCursorName(String cursorName) throws SQLException {
        Driver.unused();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        try {
            String[] strings = sql.split(";");
            for(String item : strings){
                if(item.length()>0){
                    if(item.startsWith("insert") || item.startsWith("tableInsert")){
                        objectQueue.offer(tableInsert(item).getInt());
                    }else if(item.startsWith("update")||item.startsWith("delete")){
                        //todo update delete api return row
                        connection.run(item);
                    }else{
                        Entity entity = connection.run(item);
                        if(entity instanceof  BasicTable){
                            ResultSet resultSet_ = new JDBCResultSet(connection,this,entity,item);
                            resultSets.offerLast(resultSet_);
                            objectQueue.offer(new JDBCResultSet(connection,this,entity,item));
                        }
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
        }catch (Exception e){
            throw new SQLException(e.getMessage());
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
        while (!resultSets.isEmpty()){
            ResultSet resultSet_ = resultSets.pollFirst();
            if(resultSet_ != null){
                resultSet_.close();
            }
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
    public void setFetchDirection(int fetchDirection) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getFetchSize() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batch.append(sql).append(";\n");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.delete(0,batch.length());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        List<Integer> int_list = new ArrayList<>();
        try {
            String[] strings = batch.toString().split(";");
            System.out.println(strings.length);
            for(String item : strings){
                if(item.length()>0){
                    System.out.println(item);
                    if(item.startsWith("insert") || item.startsWith("tableInsert")){
                        int_list.add(tableInsert(item).getInt());
                    }else if(item.startsWith("update")||item.startsWith("delete")){
                        //todo update delete api return row
                        connection.run(item);
                    }else{
                        Entity entity = connection.run(item);
                        if(entity instanceof  BasicTable){
                            int size = int_list.size();
                            int[] arr_int = new int[size];
                            for(int i=0; i<size; ++i){
                                arr_int[i] = int_list.get(i);
                            }
                            throw new BatchUpdateException("can not return ResultSet",arr_int);
                        }
                    }
                }
            }
            int size = int_list.size();
            int[] arr_int = new int[size];
            for(int i=0; i<size; ++i){
                arr_int[i] = int_list.get(i);
            }
            return arr_int;
        }catch (Exception e){
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }


    @Override
    public boolean getMoreResults(int current) throws SQLException{
        checkClosed();
        switch (current){
            case Statement.CLOSE_ALL_RESULTS:
                while (!resultSets.isEmpty()){
                    ResultSet resultSet_ = resultSets.pollLast();
                    if(resultSet_ != null){
                        resultSet_.close();
                    }
                }
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                if(resultSet != null){
                    resultSet.close();
                }
                break;
            case Statement.KEEP_CURRENT_RESULT:
                break;
                default:
                throw new SQLException("the argument supplied is not one of the following:\n" +
                        "Statement.CLOSE_CURRENT_RESULT,\n" +
                        "Statement.KEEP_CURRENT_RESULT or\n" +
                        " Statement.CLOSE_ALL_RESULTS");
        }
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException{
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException{
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException{
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        Driver.unused();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        Driver.unused();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        Driver.unused();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        Driver.unused();
        return false;
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

    protected BasicInt tableInsert(String sql) throws Exception{
        if(sql.startsWith("tableInsert")){
            return (BasicInt)connection.run(sql);
        }else {
            String tableName = sql.substring(sql.indexOf("into") + "into".length(), sql.indexOf("values"));
            String values;
            int index = sql.indexOf(";");
            if (index == -1) {
                values = sql.substring(sql.indexOf("values") + "values".length());
            } else {
                values = sql.substring(sql.indexOf("values") + "values".length(), sql.indexOf(";"));
            }

            String new_sql = MessageFormat.format("tableInsert({0},{1})", tableName, values);
            BasicInt n = (BasicInt) connection.run(new_sql);
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
