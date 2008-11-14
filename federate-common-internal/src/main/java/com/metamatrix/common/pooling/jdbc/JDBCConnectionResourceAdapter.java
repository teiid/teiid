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

package com.metamatrix.common.pooling.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.JDBCPlatformFactory;
import com.metamatrix.common.jdbc.JDBCUtil;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.pooling.api.Resource;
import com.metamatrix.common.pooling.api.ResourceAdapter;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;

public class JDBCConnectionResourceAdapter implements ResourceAdapter {

    private boolean decryptRequired;
    private boolean tracingEnabled;

    private static AtomicLong ID_CNTR = new AtomicLong(0);

    private static final boolean DEFAULT_DECRYPTION_REQUIRED = true;

    public JDBCConnectionResourceAdapter() {
        this.decryptRequired = DEFAULT_DECRYPTION_REQUIRED;
    }

    public Resource createResource(Object physicalResource) throws ResourcePoolException{

        try {

            Connection conn = convertToConnection(physicalResource);
            JDBCPlatform jdbcPlatform = JDBCPlatformFactory.getPlatform(conn);
            
            if (jdbcPlatform.isMYSQL()) {
                // mysql, by default, does not support standard ANSI, so it must be turned on
                Statement s = conn.createStatement();
                s.execute("set SQL_MODE='ANSI'"); //$NON-NLS-1$
                s.close();
            }
            Resource resource;
            Long nl = new Long(ID_CNTR.getAndIncrement());
			resource = new JDBCConnectionResource(conn, jdbcPlatform, nl);
            LogManager.logTrace(LogCommonConstants.CTX_POOLING,"Created resource for platform " + jdbcPlatform.getClass().getName()); //$NON-NLS-1$
			LogManager.logTrace(LogCommonConstants.CTX_POOLING,"Resource " + resource.getClass().getName()); //$NON-NLS-1$
            return resource;

        } catch (Exception e) {
            throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0045, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0045));

        }

    }


    public Object createPhysicalResourceObject(ResourceDescriptor descriptor) throws ResourcePoolException{

        Properties props = descriptor.getProperties();

        if ( isDecryptRequired() ) {
            // Decrypt connection password
            try {
                props = CryptoUtil.propertyDecrypt(JDBCConnectionResource.PASSWORD, props);
            } catch ( CryptoException e ) {

	            throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0044, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0044, descriptor.getName()));

            }
        }



        // Get the JDBC properties ...
        String driverClassName = props.getProperty(JDBCConnectionResource.DRIVER);
        String protocol        = props.getProperty(JDBCConnectionResource.PROTOCOL);
        String database        = props.getProperty(JDBCConnectionResource.DATABASE);
        String username        = props.getProperty(JDBCConnectionResource.USERNAME);
        String password        = props.getProperty(JDBCConnectionResource.PASSWORD);
		String traceDebug      = props.getProperty(JDBCConnectionResource.SQL_TRACING_PROPERTY,"false"); //$NON-NLS-1$
		tracingEnabled = Boolean.valueOf(traceDebug).booleanValue(); 
		System.setProperty(JDBCConnectionResource.SQL_TRACING_PROPERTY,String.valueOf(tracingEnabled)); 

		// Check whether driverClass refers to DataSource type
		boolean isDS = isDataSource(driverClassName);
		
        Properties jdbcEnv = new Properties();
        if ( driverClassName != null ) {
            jdbcEnv.setProperty( JDBCUtil.DRIVER, driverClassName );
        }
        if ( protocol != null ) {
            jdbcEnv.setProperty( JDBCUtil.PROTOCOL, protocol );
        }
        if ( database != null ) {
            jdbcEnv.setProperty( JDBCUtil.DATABASE, database );
        }
        if ( username != null ) {
            jdbcEnv.setProperty( JDBCUtil.USERNAME, username );
        }
        if ( password != null ) {
            jdbcEnv.setProperty( JDBCUtil.PASSWORD, password );
        }

        
        StringBuffer sb = null;
        if (database.startsWith("jdbc")) {//$NON-NLS-1$
            sb = new StringBuffer(database);
        } else {
            sb = new StringBuffer("jdbc:"); //$NON-NLS-1$
            sb.append(protocol);
            sb.append(":"); //$NON-NLS-1$
            sb.append(database);
        }


        LogManager.logTrace(LogCommonConstants.CTX_POOLING,"Opening connection to JDBC " + sb.toString() + " user: " + username); //$NON-NLS-1$ //$NON-NLS-2$

        try {
        	Connection conn = null;
        	// Handle DataSource Connection
            if(isDS) {
            	conn = JDBCUtil.createJDBCXAConnection(jdbcEnv);
            	
            // Handle JDBC Connection
            } else {
                conn = JDBCUtil.createJDBCConnection(jdbcEnv);
            }

            return conn;

        } catch (Exception e) {
	        throw new ResourcePoolException(e, ErrorMessageKeys.POOLING_ERR_0046, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0046, descriptor.getName()));

        }

    }
    
    /**
     * Check whether the driver className refers to DataSource 
     * @param driverClassName the driver className property
     * @return boolean 'true' if driver className is datasource type 
     */
    private boolean isDataSource(String driverClassName) throws ResourcePoolException {
    	boolean isDS = false;
    	if(driverClassName!=null) {
	        // create driverClass object
	        Object driverClass = null;
	        try {
	        	driverClass = Class.forName(driverClassName).newInstance();
	        } catch(Exception e) {
	            LogManager.logError(LogCommonConstants.CTX_POOLING,"Unable to load the JDBC driver class " + driverClassName); //$NON-NLS-1$
	            throw new ResourcePoolException(e, "Unable to load the JDBC driver class " + driverClassName); //$NON-NLS-1$
	        }
	        
	        if(driverClass instanceof DataSource) {
	        	isDS = true;
	        }
    	}  
    	return isDS;
    }

    public void closePhyicalResourceObject(Resource resource) throws ResourcePoolException {
        if (resource == null) {
                return;
        }

              if (resource instanceof JDBCConnectionResource) {

                    JDBCConnectionResource conn = (JDBCConnectionResource) resource;

                    if (conn.isResourceAlive()) {
                        try {
                                Connection connection = conn.getConnection();

                                connection.close();

                        } catch (SQLException e) {
							// an exception is not thrown because the physical resource is being
							// removed and will no longer cause a problme
                            I18nLogManager.logError(LogCommonConstants.CTX_POOLING, ErrorMessageKeys.POOLING_ERR_0048, e, new Object[] {CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0048, resource.getCheckedOutBy())});
                        }
                    }
              } else {
	        	  throw new ResourcePoolException(ErrorMessageKeys.POOLING_ERR_0047, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0047,resource.getClass().getName()));
              }

    }

    private Connection convertToConnection(Object obj) throws ResourcePoolException {
          if (obj instanceof Connection) {
              return (Connection) obj;
          }
          throw new ResourcePoolException(ErrorMessageKeys.POOLING_ERR_0049, CommonPlugin.Util.getString(ErrorMessageKeys.POOLING_ERR_0049, obj.getClass().getName()));
    }

    /**
     * Returns the decryptRequired.
     * @return boolean
     */
    public boolean isDecryptRequired() {
        return decryptRequired;
    }

    /**
     * Sets the decryptRequired.
     * @param decryptRequired The decryptRequired to set
     */
    protected void setDecryptRequired(final boolean decryptRequired) {
        this.decryptRequired = decryptRequired;
    }

}
