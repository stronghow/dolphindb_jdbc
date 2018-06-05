package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public  class JDBCConnection implements Connection {

    private DBConnection dbConnection;
    private String hostName;
    private int port;
    private boolean success;
    private Vector databases;
    private HashMap<String,Boolean> loadTable;


    public JDBCConnection(Properties prop) throws SQLException{
        dbConnection = new DBConnection();
        hostName = prop.getProperty("hostName");
        port = Integer.parseInt(prop.getProperty("port"));
        try {
            open(hostName,port,prop);
        }catch (Exception e){
            e.printStackTrace();
            String s = e.getMessage();
            if(s.contains("Connection refused")){
                throw new SQLException(MessageFormat.format("{0}  ==> hostName = {1}, port = {2}",s,hostName,port));
            }else{
                throw new SQLException(s);
            }

        }
    }

    private void open(String hostname, int port, Properties prop) throws SQLException, IOException{
        success = dbConnection.connect(hostname, port);
        // database(directory, [partitionType], [partitionScheme], [locations])
        if(!success) throw new SQLException("Connection is fail");
        String[] keys = new String[]{"databasePath","partitionType","partitionScheme","locations"};
        String[] values = Utils.getProperties(prop,keys);
        if(values[0] != null && values[0].length() > 0){
            values[0] = "\""+values[0]+"\"";
            System.out.println(values[0]);
            StringBuilder sb = new StringBuilder(Driver.DB).append(" = database(");
            Utils.joinOrder(sb,values,",");
            sb.append(");\n");
            sb.append("getTables(").append(Driver.DB).append(")");
            databases = (com.xxdb.data.Vector) dbConnection.run(sb.toString());
            if(values[0].trim().startsWith("dfs://")) {
                StringBuilder loadTableSb = new StringBuilder();
                for (int i = 0, len = databases.rows(); i < len; ++i) {
                    String name = databases.get(i).getString();
                    loadTableSb.append(name).append(" = ").append("loadTable(").append(Driver.DB).append(",`").append(name).append(");\n");
                }
                dbConnection.run(loadTableSb.toString());
            }
            //loadTable = new HashMap<>(databases.rows());
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkIsClosed();
        return new JDBCStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new JDBCPrepareStatement(this,sql);
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        checkIsClosed();
        return s;
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {
        checkIsClosed();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkIsClosed();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        Driver.unused();
    }

    @Override
    public void rollback() throws SQLException {
        Driver.unused();
    }

    @Override
    public void close() throws SQLException {
        if (isClosed())
            return;

        dbConnection.close();
        dbConnection = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return dbConnection == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        checkIsClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkIsClosed();
        return false;
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        Driver.unused();
    }

    @Override
    public String getCatalog() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        Driver.unused();
        return 0;
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException{
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException{
        Driver.unused();
    }

    @Override
    public int getHoldability() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException{
        Driver.unused();
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        Driver.unused();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        Driver.unused();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException{
        checkIsClosed();
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException{
        checkIsClosed();
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkIsClosed();
        Driver.unused();
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException{
        checkIsClosed();
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException{
        checkIsClosed();
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException{
        checkIsClosed();
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException{
        Driver.unused();
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException{
        throw new SQLClientInfoException("unused",null);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException{
        throw new SQLClientInfoException("unused",null);
    }

    @Override
    public String getClientInfo(String name) throws SQLException{
        Driver.unused();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException{
        Driver.unused();
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] elements) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException{
        Driver.unused();
    }

    @Override
    public String getSchema() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException{
        Driver.unused();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        checkIsClosed();
        return aClass.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        checkIsClosed();
        return aClass.isInstance(this);
    }

    private void checkIsClosed() throws SQLException{
        if(dbConnection ==null) throw new SQLException("dbConnection is null");

        if(this==null || this.isClosed()) throw new SQLException("connection isClosed");
    }

    public DBConnection getDbConnection() {
        return dbConnection;
    }
    
}
