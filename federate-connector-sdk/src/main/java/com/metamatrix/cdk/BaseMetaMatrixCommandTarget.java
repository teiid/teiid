/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.cdk;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.classloader.ClassLoaderUtil;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtilities;
import com.metamatrix.jdbc.EmbeddedDriver;
import com.metamatrix.jdbc.MMDatabaseMetaData;
import com.metamatrix.jdbc.MMDriver;
import com.metamatrix.jdbc.api.DefaultDisplayHelper;
import com.metamatrix.jdbc.api.DisplayHelper;
import com.metamatrix.jdbc.api.PlanNode;
import com.metamatrix.jdbc.api.XMLOutputVisitor;

/**
 * Implements commands for the MetaMatrixShell.
 */
public class BaseMetaMatrixCommandTarget extends QueryCommandTarget {
	
	static {
		new MMDriver();
		new EmbeddedDriver();
	}
	
    private String sql;
    protected String jdbcUrl;
    protected Connection connection = null;
    protected long connectionStartTime;
    protected long elapsedTime;
    protected long disconnectStartTime;
    private Integer queryTimeout = null;
   
    
    private ResultSet results;
    private boolean automaticallyGetResultSetMetadata = false;
    private String resultSetMetadata;
   
    private boolean ignoreQueryPlan = false;
    private Map parametersMap = new HashMap();
    protected Map parameterTypesMap = new HashMap();
    private StringBuffer resultString;
    
    protected Driver driver;
    private Properties connectionProperties;
    
    // something like "jdbc:metamatrix:QueryTest@mm://slntmm01:40131;version=1;user=MetaMatrixAdmin;password=mm"
    public void setConnectionUrl(String url) {
        jdbcUrl = url;
    }
    
    public void setConnection(String server, String port, String vdb, String version, String user, String password) {
        jdbcUrl = "jdbc:metamatrix:" + vdb + "@mm://" + server + ":" + port + ";version=" + version + ";user=" + user + ";password=" + password; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    }

    public void setSecureConnection(String server, String port, String vdb, String version, String user, String password) {
        jdbcUrl = "jdbc:metamatrix:" + vdb + "@mms://" + server + ":" + port + ";version=" + version + ";user=" + user + ";password=" + password; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    }

    public void setLocalConfig(String vdbName, String vdbVersion, String localConfigFilePath) {
        jdbcUrl = "jdbc:metamatrix:" + vdbName + "@" +localConfigFilePath+";version=" + vdbVersion; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /* 
     * @see com.metamatrix.cdk.QueryCommandTarget#execute(java.lang.String)
     */
    public String execute(String query) {
        sql = query;
        loadDriver();
        return executeQuery( new SqlRunnable() {
            public void run() throws SQLException {
                executeStatement();
            }
        });
    }

    public void setQueryTimeout(int timeoutInSeconds) {
        queryTimeout = new Integer(timeoutInSeconds);
    }
    
    public void clearQueryTimeout() {
        queryTimeout = null;
    }
    
    public void setPrintStackOnError(boolean printStackOnError) {
        shell.setPrintStackTraceOnException(printStackOnError);
    }
    
    public void setFailOnError(boolean failOnError) {
        if (failOnError) {
            shell.turnOffExceptionHandling();
        } else {
            shell.turnOnExceptionHandling();
        }        
    }
    
    public void setAutoGetMetadata(boolean automaticallyGetMetadata) {
        automaticallyGetResultSetMetadata = automaticallyGetMetadata;
    }
    
    public void connect() {
        loadDriver();
        if (connection == null) {
            try {                
                getConnection();
            } catch (SQLException e) {
                throw new MetaMatrixRuntimeException(e);
            }
        }
    }

    private void getConnection() throws SQLException {
        loadDriver();
        String driverName = "<unknown>"; //$NON-NLS-1$
        try {
            driverName = DriverManager.getDriver(jdbcUrl).getClass().getName();
        } catch (SQLException e) {
            //ignore this
        }
        shell.writeln("Connecting with " + driverName + " to " + jdbcUrl); //$NON-NLS-1$ //$NON-NLS-2$
        connectionStartTime = System.currentTimeMillis();
        try {
            
            if (connectionProperties == null) {
                connection = DriverManager.getConnection(jdbcUrl);                
            } else {
                connection = DriverManager.getConnection(jdbcUrl, connectionProperties);
            }
        } finally {
            elapsedTime = System.currentTimeMillis() - connectionStartTime;
            shell.writeln( elapsedTime + " ms" ); //$NON-NLS-1$
        }
    }
    
    public void disconnect() {
        if (connection != null) {
            try {
                disconnectStartTime = System.currentTimeMillis();
                connection.close();
            } catch (SQLException e) {
                throw new MetaMatrixRuntimeException(e);
            } finally {
                elapsedTime = System.currentTimeMillis() - disconnectStartTime;
                shell.writeln(elapsedTime + " ms");  //$NON-NLS-1$
                elapsedTime = System.currentTimeMillis() - connectionStartTime;
                shell.writeln(elapsedTime + " ms Total"); //$NON-NLS-1$
                connection = null;
            }
        }
    }
    
    private String executeQuery(SqlRunnable runnable) {
        resultString = new StringBuffer();
        boolean needConnection = (connection == null);
        
        boolean success = false;
        try {
            if (needConnection) {
                getConnection();
            }
        
            success = executeQueryDirect(runnable);
        
        } catch(SQLException e) {
            throw new MetaMatrixRuntimeException(e);
        } finally {        
            // Close the connection to server
            if (needConnection) {
                if(connection != null) {
                    try {
                        try {
                            connection.close();
                        } catch(SQLException e) {
                            // ignore
                        } finally {
                            connection = null;
                        }
                    } catch (RuntimeException e) {
                        if (success) {
                            throw e;
                        }
                    }
                }
            }
        }
        return resultString.toString();
    }

    protected boolean executeQueryDirect(SqlRunnable runnable) throws SQLException {
        boolean success = false;
        try {
            runnable.run();
            success = true;
        } finally {
            if (results != null) {
                try {
                    try {
                        results.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                } catch (RuntimeException e) {
                    if (success) {
                        throw e;
                    }
                }
            }
        }
        return success;
    }

    private void executeStatement() throws SQLException {
        Statement statement = null;
        PreparedStatement preparedStatement = null;
        boolean success = false;
        try {
            
            String executionTechnique = " as Statement"; //$NON-NLS-1$
            if (usePreparedStatement) {
                executionTechnique = " as PreparedStatement"; //$NON-NLS-1$
                preparedStatement = connection.prepareStatement(this.sql);
                statement = preparedStatement;  
                if (automaticallyGetResultSetMetadata) {
                    saveResultsMetadata(preparedStatement.getMetaData());          
                }
            } else {
                statement = connection.createStatement();
            }
            
            if (queryTimeout != null) {
                statement.setQueryTimeout(queryTimeout.intValue());
            }
            
            shell.writeln("Executing query" + executionTechnique + ": " + this.sql); //$NON-NLS-1$ //$NON-NLS-2$
            if (usePreparedStatement) {
                if (prepareStmt) {
                    setParameters(preparedStatement, parameterTypesMap);
                }
                results = preparedStatement.executeQuery();
            } else { 
                results = statement.executeQuery(this.sql);
            }
            
            printQueryPlanIfNecessary(resultString, statement);
            
            if (usePreparedStatement) {
            } else {
                if (automaticallyGetResultSetMetadata) {
                    saveResultsMetadata(results.getMetaData());
                }
            }

            printResults();
            success = true;

        } finally {
            // Close the query
            if(statement != null) {
                try {
                    try {
                        statement.close();
                    } catch(SQLException e) {
                        // ignore
                    }
                } catch (RuntimeException e) {
                    if (success) {
                        throw e;
                    }
                }
            }
        }
    }
    
    public void prepareStatement(String statementName, String[] types) {
        prepareStmt = true;
        setUsePreparedStatement(true);
        currentPreparedStmtName = statementName;  
        parameterTypesMap.put(statementName, types);
    }

    private void setParameters(PreparedStatement ps, Map cachedStmts) throws SQLException {
        String[] types = (String[]) cachedStmts.get(currentPreparedStmtName);
        if (types != null && types.length != 0) {
            String[] params = (String[]) parametersMap.get(currentPreparedStmtName);
            for (int i = 0; i < types.length; i++) {
                Object inputObj = getParameter(types[i].toUpperCase(), params[i]);
                ps.setObject(i+1, inputObj);
            }
        }
    }
    
    private Object getParameter(String type, String inputParam) {
        Object target = null;
        if (type.equals("BYTE")) { //$NON-NLS-1$
            target = new Byte(inputParam);
        } else if (type.equals("SHORT")) { //$NON-NLS-1$
            target = new Short(inputParam);
        } else if (type.equals("INTEGER")) { //$NON-NLS-1$
            target = new Integer(inputParam);
        } else if (type.equals("LONG")) { //$NON-NLS-1$
            target = new Long(inputParam);
        } else if (type.equals("SHORT")) { //$NON-NLS-1$
            target = new Short(inputParam);
        } else if (type.equals("BOOLEAN")) { //$NON-NLS-1$
            target = new Boolean(inputParam);
        } else if (type.equals("CHARACTER")) { //$NON-NLS-1$
            char[] charArray = inputParam.toCharArray();
            target = new Character(charArray[0]);
        } else if (type.equals("FLOAT")) { //$NON-NLS-1$
            target = new Float(inputParam);
        } else if (type.equals("DOUBLE")) { //$NON-NLS-1$
            target = new Double(inputParam);
        } else if (type.equals("BIGDECIMAL")) { //$NON-NLS-1$
            target = new BigDecimal(inputParam);
        } else if (type.equals("BIGINTEGER")) { //$NON-NLS-1$
            target = new BigInteger(inputParam);
        } else if (type.equals("OBJECT") || type.equals("STRING")) { //$NON-NLS-1$ //$NON-NLS-2$
            target = inputParam;
        } // todo: blob and clob
      
        return target;
    }
    
    public String getTypeInfo() {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getTypeInfo();
            }
        });
    }
    
    public String getCatalogs() {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getCatalogs();
            }
        });
    }
    
    public String getModels(final String catalog, final String schemaPattern, final String modelPattern) {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = ((MMDatabaseMetaData)connection.getMetaData()).getModels(catalog, schemaPattern, modelPattern);
            }
        });    
    }
        
    public String getSchemas() {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getSchemas();
            }
        });
    }
    
    public String getImportedKeys(final String catalog, final String schema, final String table) {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getImportedKeys(catalog, schema, table);
            }
        });
    }
    
    public String getExportedKeys(final String catalog, final String schema, final String table) {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getExportedKeys(catalog, schema, table);
            }
        });
    }
    
    public String getTables(final String schemaPattern, final String tableNamePattern) { //, String[] types) {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getTables(null, schemaPattern, tableNamePattern, null);
            }
        });
    }
    
    public String getColumns(final String schemaPattern, final String tableNamePattern, final String columnNamePattern) {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getColumns(null, schemaPattern, tableNamePattern, columnNamePattern);
            }
        });
    }
    
    public String getPrimaryKeys(final String schema, final String table)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getPrimaryKeys(null, schema, table);
            }
        });
    }
    
    public String getExportedKeys(final String schema, final String table)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getExportedKeys(null, schema, table);
            }
        });
    }
    
    public String getImportedKeys(final String schema, final String table)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getImportedKeys(null, schema, table);
            }
        });
    }
    
    public String getCrossReference(
        final String primarySchema,
        final String primaryTable,
        final String foreignSchema,
        final String foreignTable) {
        return executeMetadataRequest(new SqlRunnable() {
            public void run() throws SQLException {
                results =
                    connection.getMetaData().getCrossReference(
                        null,
                        primarySchema,
                        primaryTable,
                        null,
                        foreignSchema,
                        foreignTable);
            }
        });
    }
    
    public String getIndexInfo(final String schema, final String table, final boolean unique, final boolean approximate)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getIndexInfo(null, schema, table, unique, approximate);
            }
        });
    }
    
    public String getUDTs(final String schemaPattern, final String typeNamePattern)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getUDTs(null, schemaPattern, typeNamePattern, null);
            }
        });
    }
        
    public String getProcedures(final String schemaPattern, final String procedureNamePattern)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getProcedures(null, schemaPattern, procedureNamePattern);
            }
        });
    }

    public String getProcedureColumns(final String schemaPattern, final String procedureNamePattern, final String columnNamePattern)  {
        return executeMetadataRequest( new SqlRunnable() {
            public void run() throws SQLException {
                results = connection.getMetaData().getProcedureColumns(null, schemaPattern, procedureNamePattern, columnNamePattern);
            }
        });
    }
        
    private String executeMetadataRequest(final SqlRunnable runnable) {
        loadDriver();
        return executeQuery( new SqlRunnable() {
            public void run() throws SQLException {
                results = null;
                runnable.run();
                if (automaticallyGetResultSetMetadata) {
                    saveResultsMetadata(results.getMetaData());
                }
                printResults();
            }
        });
    }
    
    protected void loadDriver() {
        if (driver == null) {
            try {
                driver = MMDriver.getInstance();
            } catch (NoClassDefFoundError e) {
                //ignore
            }
        }
    }
    
    public void setDriverClass(String className) {
        try {
            driver = (Driver)Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException();
        } catch (IllegalAccessException e) {
            throw new RuntimeException();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException();
        }
    }
    
    private void printQueryPlanIfNecessary(StringBuffer resultString, Statement statement) {
        if (statement instanceof com.metamatrix.jdbc.api.Statement) {
            com.metamatrix.jdbc.api.Statement mmStatement = (com.metamatrix.jdbc.api.Statement) statement;
            PlanNode plan = mmStatement.getPlanDescription();
            if (plan != null && !ignoreQueryPlan) {
                printQueryPlan(resultString, plan);
            }
        }
    }

    private void printQueryPlan(StringBuffer resultString, PlanNode plan) {
        DisplayHelper displayHelper = new DefaultDisplayHelper();
        XMLOutputVisitor visitor = new XMLOutputVisitor(displayHelper);
        visitor.visit(plan);
        resultString.append(visitor.getText());
    }

    private void printResults() throws SQLException {        
        printResults(resultString, results);
    }
    
    private void saveResultsMetadata(final ResultSetMetaData metadata) throws SQLException {
        final int rowCount = metadata.getColumnCount();
        PrintableResults metadataWrapper = new PrintableResults() {
            private int rowIndex = 0;
            
            public int getColumnCount() throws SQLException {
                return 8;
            }

            public String getColumnName(int columnIndex) throws SQLException {
                switch (columnIndex) {
                    case 1 :
                        return "ColumnName"; //$NON-NLS-1$
                    case 2 :
                        return "ColumnType"; //$NON-NLS-1$
                    case 3 :
                        return "ColumnTypeName"; //$NON-NLS-1$
                    case 4 :
                        return "ColumnClassName"; //$NON-NLS-1$
                    case 5 :
                        return "isNullable"; //$NON-NLS-1$
                    case 6 :
                        return "TableName"; //$NON-NLS-1$
                    case 7 :
                        return "SchemaName"; //$NON-NLS-1$
                    case 8 :
                        return "CatalogName"; //$NON-NLS-1$
                }
                throw new IndexOutOfBoundsException();
            }

            public boolean next() throws SQLException {
                rowIndex++;
                return rowIndex <= rowCount;
            }

            public Object getObject(int columnIndex) throws SQLException {
                switch (columnIndex) {
                    case 1 :
                        return metadata.getColumnName(rowIndex);
                    case 2 :
                        return new Integer(metadata.getColumnType(rowIndex));
                    case 3 :
                        return metadata.getColumnTypeName(rowIndex);
                    case 4 :
                        return metadata.getColumnClassName(rowIndex);
                    case 5 :
                        return new Integer( metadata.isNullable(rowIndex) );
                    case 6 :
                        return metadata.getTableName(rowIndex);
                    case 7 :
                        return metadata.getSchemaName(rowIndex);
                    case 8 :
                        return metadata.getCatalogName(rowIndex);
                }
                throw new IndexOutOfBoundsException();
            }
        };
        StringBuffer metadataResults = new StringBuffer();
        printResultsDirect(metadataResults, metadataWrapper);
        resultSetMetadata = metadataResults.toString();
    }

    public String getResultSetMetadata() {
        return resultSetMetadata;
    }

    public static void printResults(StringBuffer resultString, final ResultSet results)
        throws SQLException {
        
        PrintableResults resultsWrapper = new PrintableResults() {
            public int getColumnCount() throws SQLException {
                return results.getMetaData().getColumnCount();
            }

            public String getColumnName(int columnIndex) throws SQLException {
                return results.getMetaData().getColumnName(columnIndex);
            }

            public boolean next() throws SQLException {
                return results.next();
            }

            public Object getObject(int columnIndex) throws SQLException {
                Object obj = results.getObject(columnIndex);
                if (obj instanceof SQLXML) {
                    return ((SQLXML)obj).getString();
                }
                return obj;
            }
        };
        printResultsDirect(resultString, resultsWrapper);
    }
    
    public static void printResultsDirect(StringBuffer resultString, PrintableResults results)
        throws SQLException {

        // Print column names using results metadata
        int columnCount = results.getColumnCount();
        for(int col=1; col<=columnCount; col++) {
            resultString.append(results.getColumnName(col) + "\t"); //$NON-NLS-1$
        }
        resultString.append(StringUtilities.LINE_SEPARATOR); 

        // Walk through each row of results
        for(int row=1; results.next(); row++) {
            // Walk through column values in this row
            int i=1;
            for(; i <= (columnCount-1); i++) {
                resultString.append(results.getObject(i) + "\t"); //$NON-NLS-1$
            }
            resultString.append(results.getObject(i));
            resultString.append(StringUtilities.LINE_SEPARATOR); 
        }
    }
    
    public String runStatement(String statementName, String[] params) throws SQLException {
        if (!preparedStmts.containsKey(statementName)) {
            shell.writeln("Wrong Prepared Statement Name " + statementName); //$NON-NLS-1$
        }
        String[] actualParams = null;
        if (params != null) {
            actualParams = new String[params.length];
            for (int i = 0; i < params.length; i++) {
                if (params[i] != null && params[i].equalsIgnoreCase("null")) { //$NON-NLS-1$
                    actualParams[i] = null;
                } else {
                    actualParams[i] = params[i];
                }
            }
        }
        parametersMap.put(statementName, actualParams);
        currentPreparedStmtName = statementName;
        return execute((String) preparedStmts.get(statementName));
    }
    
    interface SqlRunnable {
        void run() throws SQLException;
    }
    
    interface PrintableResults {
        int getColumnCount() throws SQLException;
        String getColumnName(int columnIndex) throws SQLException;
        boolean next() throws SQLException;
        Object getObject(int columnIndex) throws SQLException;
    }
    
    public String exec(String[] args) {
        return executeSql("exec", args); //$NON-NLS-1$
    }
    
    public void setIgnoreQueryPlan(boolean ignoreQueryPlan) {
        this.ignoreQueryPlan = ignoreQueryPlan;
    }
    
    public String printClassPath() {
        shell.writeln( ClassLoaderUtil.getClassLoaderInformation( this.getClass().getClassLoader(), "shell" ) ); //$NON-NLS-1$
        return System.getProperty("java.class.path"); //$NON-NLS-1$
    }

    public void printContextClassPath() {
        shell.writeln( ClassLoaderUtil.getClassLoaderInformation( Thread.currentThread().getContextClassLoader(), "Thread context class loader" ) ); //$NON-NLS-1$
    }

    public void printLoadedClasses() {
        ClassLoaderUtil.printLoadedClasses(this.getClass().getClassLoader(), System.out);
    }
    
    public void printConnectionClasses() {
        ClassLoaderUtil.printLoadedClasses(connection.getClass().getClassLoader(), System.out);
    }
    
    public void setClassLoaderDebug(boolean value) {
        ClassLoaderUtil.debug = value;
    }

    public void setAutoCommit(boolean value) throws SQLException {
        connection.setAutoCommit(value);
    }
    
    public void commit() throws SQLException {
        connection.commit();
    }
    
    public void rollback() throws SQLException {
        connection.rollback();
    }
    
    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }
    
    public void sleep(String waitTimeString) {
        long waitTime = Long.parseLong(waitTimeString);
        try {       
            Thread.sleep(waitTime);
        } catch (InterruptedException ie) {}
    }
}
