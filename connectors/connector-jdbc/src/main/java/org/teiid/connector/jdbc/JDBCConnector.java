/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package org.teiid.connector.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MappedUserIdentity;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.api.SingleIdentity;
import org.teiid.connector.api.ConnectorAnnotations.ConnectionPooling;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.connector.xa.api.XAConnection;
import org.teiid.connector.xa.api.XAConnector;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;

/**
 * JDBC implementation of Connector interface.
 */
@ConnectionPooling
public class JDBCConnector extends BasicConnector implements XAConnector, MetadataProvider {
	
	static final int NO_ISOLATION_LEVEL_SET = Integer.MIN_VALUE;

	enum TransactionIsolationLevel {
        TRANSACTION_READ_UNCOMMITTED(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED),
        TRANSACTION_READ_COMMITTED(java.sql.Connection.TRANSACTION_READ_COMMITTED),
        TRANSACTION_REPEATABLE_READ(java.sql.Connection.TRANSACTION_REPEATABLE_READ),
        TRANSACTION_SERIALIZABLE(java.sql.Connection.TRANSACTION_SERIALIZABLE),
        TRANSACTION_NONE(java.sql.Connection.TRANSACTION_NONE);

        private int connectionContant;

        private TransactionIsolationLevel(int connectionConstant) {
			this.connectionContant = connectionConstant;
		}
        
        public int getConnectionConstant() {
        	return connectionContant;
        }
	}
	
	protected ConnectorEnvironment environment;
    private ConnectorLogger logger;
    private ConnectorCapabilities capabilities;
    private Translator sqlTranslator;
    private DataSource ds;
    private XADataSource xaDs;
    private int transIsoLevel = NO_ISOLATION_LEVEL_SET;
        
    @Override
    public void start(ConnectorEnvironment environment)
    		throws ConnectorException {
    	super.start(environment);
    	logger = environment.getLogger();
        this.environment = environment;
        
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_initialized._1")); //$NON-NLS-1$
        
        Properties connectionProps = environment.getProperties();

        // Get the JDBC properties ...
        String dataSourceClassName = connectionProps.getProperty(JDBCPropertyNames.CONNECTION_SOURCE_CLASS);
        
        // Verify required items
        if (dataSourceClassName == null || dataSourceClassName.trim().length() == 0) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Missing_JDBC_driver_class_name_1")); //$NON-NLS-1$
        }
        dataSourceClassName = dataSourceClassName.trim();

        String levelStr = connectionProps.getProperty(JDBCPropertyNames.TRANSACTION_ISOLATION_LEVEL);
        
        if(levelStr != null && levelStr.trim().length() != 0){
        	transIsoLevel = TransactionIsolationLevel.valueOf(levelStr.toUpperCase()).getConnectionConstant();
        }
        
        try {
            String className = environment.getProperties().getProperty(JDBCPropertyNames.EXT_TRANSLATOR_CLASS, Translator.class.getName());  
            this.sqlTranslator = (Translator)ReflectionHelper.create(className, null, Thread.currentThread().getContextClassLoader());
        } catch (MetaMatrixCoreException e) {
            throw new ConnectorException(e);
        }
        sqlTranslator.initialize(environment);
        
        capabilities = sqlTranslator.getConnectorCapabilities();
        
        createDataSources(dataSourceClassName, connectionProps);
        
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_started._4")); //$NON-NLS-1$
    }
    
	@Override
    public void stop() {     
		/*
		 * attempt to deregister drivers that may have been implicitly registered
		 * with the driver manager
		 */
		boolean usingCustomClassLoader = PropertiesUtils.getBooleanProperty(this.environment.getProperties(), ConnectorPropertyNames.USING_CUSTOM_CLASSLOADER, false);

		if (!usingCustomClassLoader) {
			return;
		}
		
		Enumeration drivers = DriverManager.getDrivers();

        while(drivers.hasMoreElements()){
        	Driver tempdriver = (Driver)drivers.nextElement();
            if(tempdriver.getClass().getClassLoader() != Thread.currentThread().getContextClassLoader()) {
            	continue;
            }
            try {
                DriverManager.deregisterDriver(tempdriver);
            } catch (Throwable e) {
                this.environment.getLogger().logError(e.getMessage());
            }
        }
                
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_stopped._3")); //$NON-NLS-1$
    }
    
	@Override
    public Connection getConnection(ExecutionContext context) throws ConnectorException {
		DataSource dataSource = getDataSource();
		if (dataSource == null) {
			return getXAConnection(context, null);
		}
		java.sql.Connection conn = null;
		try { 
			if (context == null || context.getConnectorIdentity() instanceof SingleIdentity) {
				conn = dataSource.getConnection();
			} else if (context.getConnectorIdentity() instanceof MappedUserIdentity) {
				MappedUserIdentity id = (MappedUserIdentity)context.getConnectorIdentity();
				conn = dataSource.getConnection(id.getMappedUser(), id.getPassword());
			}
			setDefaultTransactionIsolationLevel(conn);
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
		return createJDBCSourceConnection(conn, this.environment, this.sqlTranslator);
    }

	public Connection createJDBCSourceConnection(java.sql.Connection conn, ConnectorEnvironment env, Translator trans)
			throws ConnectorException {
		return new JDBCSourceConnection(conn, env, trans);
	}
	
	@Override
	public XAConnection getXAConnection(
			ExecutionContext context,
			TransactionContext transactionContext) throws ConnectorException {
		XADataSource xaDataSource = getXADataSource();
		if (xaDataSource == null) {
			throw new UnsupportedOperationException(JDBCPlugin.Util.getString("JDBCConnector.non_xa_connection_source")); //$NON-NLS-1$
		}
		javax.sql.XAConnection conn = null;
		try {
			if (context == null || context.getConnectorIdentity() instanceof SingleIdentity) {
				conn = xaDataSource.getXAConnection();
			} else if (context.getConnectorIdentity() instanceof MappedUserIdentity) {
				MappedUserIdentity id = (MappedUserIdentity)context.getConnectorIdentity();
				conn = xaDataSource.getXAConnection(id.getMappedUser(), id.getPassword());
			} else {
				throw new ConnectorException(JDBCPlugin.Util.getString("JDBCConnector.unsupported_identity_type")); //$NON-NLS-1$
			}
			java.sql.Connection c = conn.getConnection();
			setDefaultTransactionIsolationLevel(c);
			return createJDBCSourceXAConnection(conn, c, this.environment, this.sqlTranslator);
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
	}

	public XAConnection createJDBCSourceXAConnection(
			javax.sql.XAConnection conn, java.sql.Connection c, ConnectorEnvironment env, Translator trans)
			throws ConnectorException, SQLException {
		return new JDBCSourceXAConnection(c, conn, env, trans);
	}

    @Override
	public ConnectorCapabilities getCapabilities() {
		return capabilities;
	}
	
    protected void createDataSources(String dataSourceClassName, final Properties connectionProps) throws ConnectorException {
        // create data source
        Object temp = null;
        try {
        	temp = ReflectionHelper.create(dataSourceClassName, null, Thread.currentThread().getContextClassLoader());
        } catch (MetaMatrixCoreException e) {
    		throw new ConnectorException(e,JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Unable_to_load_the_JDBC_driver_class_6", dataSourceClassName)); //$NON-NLS-1$
    	}

        final String url = connectionProps.getProperty(JDBCPropertyNames.URL);

        if (temp instanceof Driver) {
    		final Driver driver = (Driver)temp;
    		// check URL if there is one
            if (url == null || url.trim().length() == 0) {
                throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Missing_JDBC_database_name_3")); //$NON-NLS-1$
            }
            validateURL(driver, url);
    		this.ds = (DataSource)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {DataSource.class}, new InvocationHandler() {
    			@Override
    			public Object invoke(Object proxy, Method method,
    					Object[] args) throws Throwable {
    				if (method.getName().equals("getConnection")) { //$NON-NLS-1$
    					Properties p = new Properties();
    					String user = null;
    					String password = null;
    					if (args != null && args.length == 2) {
    						user = (String)args[0];
    						password = (String)args[1];
    					} else {
    						user = connectionProps.getProperty(JDBCPropertyNames.USERNAME);
    						password = connectionProps.getProperty(JDBCPropertyNames.PASSWORD);
    					}
    					if (user != null) {
    						p.put("user", user); //$NON-NLS-1$
    					}
    					if (password != null) {
    						p.put("password", password); //$NON-NLS-1$
    					}
    					return driver.connect(url, p);
    				} 
    				throw new UnsupportedOperationException("Driver DataSource proxy only provides Connections"); //$NON-NLS-1$
    			}
    		});
    	} else {
    		if (temp instanceof DataSource) {
	    		this.ds = (DataSource)temp;
	            PropertiesUtils.setBeanProperties(this.ds, connectionProps, null);
    		} else if (temp instanceof XADataSource) {
    			this.xaDs = (XADataSource)temp;
    	        PropertiesUtils.setBeanProperties(this.xaDs, connectionProps, null);
    		} else {
    			throw new ConnectorException(JDBCPlugin.Util.getString("JDBCConnector.invalid_source", dataSourceClassName)); //$NON-NLS-1$
    		}
    	} 
    	if (this.ds instanceof XADataSource) {
    		this.xaDs = (XADataSource)this.ds;
    	}
    }
    
    public DataSource getDataSource() {
    	return ds;
    }
    
    public XADataSource getXADataSource() {
		return xaDs;
	}
        
    private void validateURL(Driver driver, String url) throws ConnectorException {
        boolean acceptsURL = false;
        try {
            acceptsURL = driver.acceptsURL(url);
        } catch ( SQLException e ) {
            throw new ConnectorException(e);
        }
        if(!acceptsURL ){
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Driver__7", driver.getClass().getName(), url)); //$NON-NLS-1$
        }
    }
    
    public int getDefaultTransactionIsolationLevel() {
        return this.transIsoLevel;
    }
    
	protected void setDefaultTransactionIsolationLevel(java.sql.Connection sqlConn)
			throws SQLException {
		if(getDefaultTransactionIsolationLevel() != NO_ISOLATION_LEVEL_SET && getDefaultTransactionIsolationLevel() != java.sql.Connection.TRANSACTION_NONE){
		    sqlConn.setTransactionIsolation(getDefaultTransactionIsolationLevel());
		}
	}
	
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory)
			throws ConnectorException {
		java.sql.Connection conn = null;
		javax.sql.XAConnection xaConn = null;
		try {
			if (ds != null) {
				conn = ds.getConnection();
			} else {
				xaConn = xaDs.getXAConnection();
				conn = xaConn.getConnection();
			}
			JDBCMetdataProcessor metadataProcessor = new JDBCMetdataProcessor();
			PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getImportProperties(), "importer"); //$NON-NLS-1$
			PropertiesUtils.setBeanProperties(metadataProcessor, this.environment.getProperties(), "importer"); //$NON-NLS-1$
			metadataProcessor.getConnectorMetadata(conn, metadataFactory);
		} catch (SQLException e) {
			throw new ConnectorException(e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (xaConn != null) {
					xaConn.close();
				}
			} catch (SQLException e) {
				
			}
		}
	}
        
}
