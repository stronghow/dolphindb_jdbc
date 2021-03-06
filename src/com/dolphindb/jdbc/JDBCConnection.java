package com.dolphindb.jdbc;


import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;

public  class JDBCConnection implements Connection {
    private DBConnection controlConnection;
    private DBConnection dbConnection;
    private String hostName;
    private int port;
    private boolean success;
    private Vector databases;
    private String url;
    private DatabaseMetaData metaData;
    private List<String> hostName_ports;
    private boolean isDFS;
    private StringBuilder sqlSb;


    public JDBCConnection(String url,Properties prop) throws SQLException{
        this.url = url;
        dbConnection = new DBConnection();
        hostName = prop.getProperty("hostName");
        port = Integer.parseInt(prop.getProperty("port"));
        try {
            open(hostName,port,prop);
        }catch (IOException e){
            e.printStackTrace();
            String s = e.getMessage();
            if(s.contains("Connection refused")){
                throw new SQLException(MessageFormat.format("{0}  ==> hostName = {1}, port = {2}",s,hostName,port));
            }else{
                throw new SQLException(e);
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
            StringBuilder sb = new StringBuilder(Driver.DB).append(" = database(");
            Utils.joinOrder(sb,values,",");
            sb.append(");\n");
            sqlSb = new StringBuilder();
            sqlSb.append(sb);
            dbConnection.run(sb.toString());

            if(values[0].trim().startsWith("\"dfs://")) {
                isDFS = true;
                databases = (Vector) dbConnection.run("getTables("+Driver.DB+")");
                StringBuilder loadTableSb = new StringBuilder();
                for (int i = 0, len = databases.rows(); i < len; ++i) {
                    String name = databases.get(i).getString();
                    loadTableSb.append(name).append(" = ").append("loadTable(").append(Driver.DB).append(",`").append(name).append(");\n");
                }

                sqlSb.append(loadTableSb);
                String sql = loadTableSb.toString();
                dbConnection.run(sql);
            }

            String controllerAlias = dbConnection.run("getControllerAlias()").getString();
            if(controllerAlias != null && controllerAlias.length() > 0){
                isDFS = true;
                String controlHost = dbConnection.run("rpc(\""+controllerAlias+"\", getNodeHost)").getString();
                int controlPort = ((BasicInt) dbConnection.run("rpc(\""+controllerAlias+"\", getNodePort)")).getInt();
                controlConnection = new DBConnection();
                controlConnection.connect(controlHost,controlPort);
                BasicTable table = (BasicTable) controlConnection.run("getClusterChunkNodesStatus()");
                Vector siteVector = table.getColumn("site");
                hostName_ports = new LinkedList<>();
                for (int i = 0, len = siteVector.rows(); i < len; i++) {
                    hostName_ports.add(siteVector.get(i).getString());
                }
            }else{
                isDFS = false;
            }
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
    public CallableStatement prepareCall(String sql) throws SQLException {
        Driver.unused("prepareCall(String sql)");
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
        Driver.unused("commit()");
    }

    @Override
    public void rollback() throws SQLException {
        Driver.unused("rollback()");
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        dbConnection.close();
        dbConnection = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return dbConnection == null;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkIsClosed();
        if(metaData == null){
            metaData  = new JDBCDataBaseMetaData(this,null);
        }
        return metaData;
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
    public void setCatalog(String catalog) throws SQLException{
        Driver.unused("setCatalog(String catalog");
    }

    @Override
    public String getCatalog() throws SQLException {
        Driver.unused("getCatalog()");
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException{
        Driver.unused("setTransactionIsolation(int level)");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        Driver.unused("getTransactionIsolation()");
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        Driver.unused("getWarnings()");
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        Driver.unused("clearWarnings()");
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
        Driver.unused("prepareCall(String sql, int resultSetType, int resultSetConcurrency)");
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        Driver.unused("getTypeMap()");
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        Driver.unused("setTypeMap(Map<String, Class<?>> map)");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException{
        Driver.unused("setHoldability(int holdability)");
    }

    @Override
    public int getHoldability() throws SQLException {
        Driver.unused("getHoldability()");
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        Driver.unused("setSavepoint()");
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException{
        Driver.unused("setSavepoint(String name)");
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        Driver.unused("rollback(Savepoint savepoint");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        Driver.unused("releaseSavepoint(Savepoint savepoint)");
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
        Driver.unused("prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
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
        Driver.unused("createClob()");
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        Driver.unused("createBlob()");
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        Driver.unused("createNClob()");
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        Driver.unused("createSQLXML()");
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException{
        Driver.unused("isValid(int timeout)");
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException{
        throw new SQLClientInfoException("setClientInfo(String name, String value)",null);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException{
        throw new SQLClientInfoException("setClientInfo(Properties properties)",null);
    }

    @Override
    public String getClientInfo(String name) throws SQLException{
        Driver.unused("getClientInfo(String name)");
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        Driver.unused("getClientInfo()");
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException{
        Driver.unused("createArrayOf(String typeName, Object[] elements)");
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] elements) throws SQLException {
        Driver.unused("createStruct(String typeName, Object[] elements)");
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException{
        Driver.unused("setSchema(String schema)");
    }

    @Override
    public String getSchema() throws SQLException {
        Driver.unused("getSchema()");
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        Driver.unused("abort(Executor executor)");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException{
        Driver.unused("setNetworkTimeout(Executor executor, int milliseconds)");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        Driver.unused("getNetworkTimeout()");
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
        if(dbConnection == null) throw new SQLException("connection isClosed");

        if(this==null || this.isClosed()) throw new SQLException("connection isClosed");
    }

    //Automatic switching node
    public Entity run(String function, List<Entity> arguments) throws IOException{
        if (!isDFS){
            return this.dbConnection.run(function, arguments);
        }

        int size = hostName_ports.size();
        Entity entity = null;
        try {
            entity = this.dbConnection.run(function, arguments);
            return entity;
        }catch (IOException e){
            String message = null;
            for (int index = 0 ; index<size; ++index){
                String[] hostName_port = hostName_ports.get(index).split(":");
                System.out.println("Select "+ hostName_port[0] + ":" + hostName_port[1]);
                this.dbConnection.close();
                this.dbConnection = new DBConnection();
                try {
                    if(this.dbConnection.connect(hostName_port[0],Integer.parseInt(hostName_port[1]))){
                        System.out.println("Connect " + this.dbConnection.getHostName() + ":" + this.dbConnection.getPort());
                        this.dbConnection.run(sqlSb.toString());
                        entity =  this.dbConnection.run(function, arguments);
                        return entity;
                    }
                }catch (IOException e1){
                    message = e1.getMessage();
                }
            }
            if(message != null){
                throw new IOException(message + " or All dataNode is death");
            }else{
                throw new IOException("All dataNode is death");
            }
        }
    }

    //Automatic switching node
    public Entity run(String script) throws IOException{
        if (!isDFS){
            return this.dbConnection.run(script);
        }
        script = script.trim();
        Matcher matcher = Utils.ASSIGN_PATTERN.matcher(script);
        if(matcher.find()){
            sqlSb.append(script).append(";\n");
        }

        int size = hostName_ports.size();
        Entity entity = null;
        try {
            entity = this.dbConnection.run(script);
            return entity;
        }catch (IOException e){
            String message = null;
            for (int index = 0 ; index<size; ++index){
                String[] hostName_port = hostName_ports.get(index).split(":");
                System.out.println("Select "+ hostName_port[0] + ":" + hostName_port[1]);
                this.dbConnection.close();
                this.dbConnection = new DBConnection();
                try {
                    if(this.dbConnection.connect(hostName_port[0],Integer.parseInt(hostName_port[1]))){
                        System.out.println("Connect " + this.dbConnection.getHostName() + ":" + this.dbConnection.getPort());
                        this.dbConnection.run(sqlSb.toString());
                        entity =  this.dbConnection.run(script);
                        return entity;
                    }
                }catch (IOException e1){
                    message = e1.getMessage();
                }
            }
            if(message != null){
                throw new IOException(message + " or All dataNode is death");
            }else{
                throw new IOException("All dataNode is death");
            }
        }
    }

    public String getUrl() {
        return url;
    }

    public String getHostName() {
        if(this.dbConnection != null) {
            return this.dbConnection.getHostName();
        }else{
            return null;
        }
    }

    public int getPort() {
        if(this.dbConnection != null) {
            return this.dbConnection.getPort();
        }else{
            return -1;
        }
    }
}
