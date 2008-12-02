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

package com.metamatrix.jdbc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.api.MMURL_Properties.CONNECTION;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.jdbc.api.ConnectionProperties;
import com.metamatrix.jdbc.api.ExecutionProperties;
import com.metamatrix.jdbc.transport.MultiTransportFactory;
import com.metamatrix.jdbc.util.MMJDBCURL;

/**
 * <p> The java.sql.DriverManager class uses this class to connect to MetaMatrix.
 * The Driver Manager maintains a pool of MMDriver objects, which it could use
 * to connect to MetaMatrix. The MMDriver class has a static initializer, which
 * is used to instantiate and register itsef with java.sql.DriverManager. The
 * DriverManager's <code>getConnection</code> method calls <code>connect</code>
 * method on available registered drivers. The first driver to recognise the given
 * url is used to obtain a connection.</p>
 */

public final class MMDriver extends BaseDriver {

    static final String JDBC = BaseDataSource.JDBC;
    static final String URL_PREFIX = JDBC + BaseDataSource.METAMATRIX_PROTOCOL; 
    static final int MAJOR_VERSION = 5;
    static final int MINOR_VERSION = 5;
    static final String DRIVER_NAME = "MetaMatrix JDBC Driver"; //$NON-NLS-1$
    /**
     *  Suports JDBC URLS of format
     *  - jdbc:metamatrix:BQT@mm://localhost:####;version=1
     *  - jdbc:metamatrix:BQT@mms://localhost:####;version=1
     *  - jdbc:metamatrix:BQT@mm(s)://host1:####,host2:####,host3:####;version=1
     */
    
    // This host/port pattern allows just a . or a - to be in the host part.
    static final String HOST_PORT_PATTERN = "[\\p{Alnum}\\.\\-]+:\\d+"; //$NON-NLS-1$
    static final String URL_PATTERN = "jdbc:metamatrix:(\\w+)@((mm[s]?://"+HOST_PORT_PATTERN+"(,"+HOST_PORT_PATTERN+")*)[;]?){1}((.*)*)"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    static Pattern urlPattern = Pattern.compile(URL_PATTERN);
    
    static MultiTransportFactory CONNECTION_FACTORY = new MultiTransportFactory();

    private static MMDriver INSTANCE = new MMDriver();
        
    // Static initializer
    static {

        try {
            ApplicationInfo info = ApplicationInfo.getInstance();
            info.setMainComponent("metamatrix-jdbc.jar"); //$NON-NLS-1$
            info.markUnmodifiable();
        } catch (Exception e) {
            String logMsg = JDBCPlugin.Util.getString("MMDriver.Err_init_appinfo", e.getMessage()); //$NON-NLS-1$
            DriverManager.println(logMsg);
        }
        
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch(SQLException e) {
            // Logging
            String logMsg = JDBCPlugin.Util.getString("MMDriver.Err_registering", e.getMessage()); //$NON-NLS-1$
            DriverManager.println(logMsg);
        }
    }

    public static MMDriver getInstance() {
        return INSTANCE;
    }
    
    /**
     * Should be a singleton and only constructed in {@link #getInstance}.
     */
    public MMDriver() {
        // this is not singleton, if you want singleton make this private.
    }

    /**
     * This method tries to make a metamatrix connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection to the MetaMatrix server.
     */
    public Connection connect(String url, Properties info) throws SQLException {

        MMConnection myConnection = null;
        // create a properties obj if it is null
        if(info == null) {
            info = new Properties();
        } else {
            info = PropertiesUtils.clone(info);
        }

        // The url provided is in the correct format.
        if (!acceptsURL(url)) {
        	return null;
        }

        try {
            // parse the URL to add it's properties to properties object
            parseURL(url, info);

            myConnection = createMMConnection(url, info);
        } catch (MetaMatrixCoreException e) {
            DriverManager.println(e.getMessage());
            throw MMSQLException.create(e, e.getMessage());
            //throw new MMSQLException(e.getMessage(), e);
        }

        // logging
        String logMsg = JDBCPlugin.Util.getString("MMDriver.Connection_sucess"); //$NON-NLS-1$
        myConnection.getLogger().log(MessageLevel.INFO, logMsg);

        return myConnection;
    }

    public MMServerConnection createMMConnection(String url, Properties info)
        throws ConnectionException, CommunicationException, LogonException {

        String transport = setupTransport(info);
        ServerConnection serverConn = CONNECTION_FACTORY.establishConnection(transport, info);

        // construct a MMConnection object.
        MMServerConnection connection = MMServerConnection.newInstance(serverConn, info, url);
        return connection;
    }

    static String setupTransport(Properties info) {
        // create a connection using the transport factory
        return MultiTransportFactory.SOCKET_TRANSPORT; 
    }

    /**
     * This method parses the URL and adds properties to the the properties object.
     * These include required and any optional properties specified in the URL.
     * Expected URL format -- jdbc:metamatrix:local:VDB@server:port;version=1;user=logon;
     * password=pw;logFile=<logFile.log>;
     * logLevel=<logLevel>;txnAutoWrap=<?>;credentials=mycredentials
     * @param The URL needed to be parsed.
     * @param The properties object which is to be updated with properties in the URL.
     * @throws SQLException if the URL is not in the expected format.
     */
    static void parseURL(String url, Properties info) throws SQLException {
        if(url == null) {
            String msg = JDBCPlugin.Util.getString("MMDriver.urlFormat"); //$NON-NLS-1$
            throw new MMSQLException(msg);
        }
        try {
            MMJDBCURL jdbcURL = new MMJDBCURL(url);
            info.setProperty(BaseDataSource.VDB_NAME, jdbcURL.getVDBName());
            info.setProperty(MMURL_Properties.SERVER.SERVER_URL, jdbcURL.getConnectionURL());
            Properties optionalParams = jdbcURL.getProperties();
            MMJDBCURL.normalizeProperties(info);
            Enumeration keys = optionalParams.keys();
            while (keys.hasMoreElements()) {
                String propName = (String)keys.nextElement();
                // Don't let the URL properties override the passed-in Properties object.
                if (!info.containsKey(propName)) {
                    info.setProperty(propName, optionalParams.getProperty(propName));
                }
            }
            if(optionalParams.containsKey(BaseDataSource.VERSION)) {
                // add the property only if it is new because they could have
                // already been specified either through url or otherwise.
                if(! info.containsKey(BaseDataSource.VDB_VERSION)) {
                    info.setProperty(BaseDataSource.VDB_VERSION, optionalParams.getProperty(BaseDataSource.VERSION));
                }
            }
            if(optionalParams.containsKey(BaseDataSource.LOG_FILE)) {
                String value = optionalParams.getProperty(BaseDataSource.LOG_FILE);
                if(value != null) {
                    try {
                        File f = new File(value);
                        boolean exists = f.exists(); 
                        FileWriter fw = new FileWriter(f, true);
                        fw.close();
                        if (!exists) {
                            f.delete();
                        }
                    } catch(IOException ioe) {
                        String msg = JDBCPlugin.Util.getString("MMDriver.Invalid_log_name", value); //$NON-NLS-1$
                        throw MMSQLException.create(ioe, msg);
                        //throw new MMSQLException(msg, ioe);
                    }
                }
            }
            if(optionalParams.containsKey(BaseDataSource.LOG_LEVEL)) {
                try {
                    int loglevel = Integer.parseInt(optionalParams.getProperty(BaseDataSource.LOG_LEVEL));
                    if(loglevel < BaseDataSource.LOG_NONE || loglevel > BaseDataSource.LOG_TRACE) {
                        Object[] params = new Object[] {new Integer(BaseDataSource.LOG_NONE), new Integer(BaseDataSource.LOG_ERROR), new Integer(BaseDataSource.LOG_INFO), new Integer(BaseDataSource.LOG_TRACE)};
                        String msg = JDBCPlugin.Util.getString("MMDriver.Log_level_invalid", params);  //$NON-NLS-1$
                        throw new MMSQLException(msg);
                    }
                } catch(NumberFormatException nfe) {
                    Object[] params = new Object[] {new Integer(BaseDataSource.LOG_NONE), new Integer(BaseDataSource.LOG_ERROR), new Integer(BaseDataSource.LOG_INFO), new Integer(BaseDataSource.LOG_TRACE)};
                    String msg = JDBCPlugin.Util.getString("MMDriver.Log_level_invalid", params);  //$NON-NLS-1$
                    throw MMSQLException.create(nfe, msg);
                    //throw new MMSQLException(msg, nfe);
                }
            }
        } catch(IllegalArgumentException iae) {
            String msg = JDBCPlugin.Util.getString("MMDriver.urlFormat"); //$NON-NLS-1$
            throw new MMSQLException(msg);
        }  
        
        createCredentialToken(info); 
    }

    /** 
     * Use the credentials property to construct a CredentialMap and turn it into the session token.
     * 
     * @param info
     * @throws MMSQLException
     * @since 4.3
     */
    static void createCredentialToken(Properties info) throws MMSQLException {
        // Handle creation of SessionToken from credentials property
        if(info.containsKey(ConnectionProperties.PROP_CREDENTIALS)) {
            // Check if both credentials AND session token are used - if so, this is an error
            if(info.containsKey(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD)) {
                throw new MMSQLException(JDBCPlugin.Util.getString("MMDriver.Invalid_use_of_credentials_and_token"));                //$NON-NLS-1$
            } 
            
            // Parse credentials and store CredentialMap as session token
            try { 
                String credentials = info.getProperty(ConnectionProperties.PROP_CREDENTIALS);
                info.put(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD, buildCredentialMap(info, credentials));
            } catch(Exception e) {
                throw MMSQLException.create(e);
            }
            
            // Remove credentials from info properties
            info.remove(ConnectionProperties.PROP_CREDENTIALS);
        }
    }

	static private HashMap buildCredentialMap(Properties info, String credentials) {
		HashMap credentialMap = new HashMap();
		boolean defaultToLogon = false;
		if(credentials.startsWith(ConnectionProperties.DEFAULT_TO_LOGON)) {
		    defaultToLogon = true;
		}
		if(defaultToLogon) {
			credentialMap.put("user", info.getProperty(BaseDataSource.USER_NAME)); //$NON-NLS-1$
			credentialMap.put("password", info.getProperty(BaseDataSource.PASSWORD)); //$NON-NLS-1$
		}
		credentialMap.put("TRUST_KEY", credentials);  //$NON-NLS-1$
		
		return credentialMap;
	}
    
    /**
     * Returns true if the driver thinks that it can open a connection to the given URL.
     * Typically drivers will return true if they understand the subprotocol specified
     * in the URL and false if they don't.
     * Expected URL format is
     * jdbc:metamatrix:subprotocol:VDB@server:port;version=1;logFile=<logFile.log>;logLevel=<logLevel>;txnAutoWrap=<?>
     * @param The URL used to establish a connection.
     * @return A boolean value indicating whether the driver understands the subprotocol.
     * @throws SQLException, should never occur
     */
    public boolean acceptsURL(String url) throws SQLException {
        Matcher m = urlPattern.matcher(url);
        return m.matches();
    }

    /**
     * This method could be used to prompt the user for properties to connect to
     * metamatrix (properties that are not already specified for obtaining connection).
     * @param The URL used to establish a connection.
     * @param A properties object containing properties needed to obtain a connection.
     * @return An array containing DriverPropertyInfo objects
     * @throws SQLException, if parsing error occurs
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if(info == null) {
            info = new Properties();
        }

        // parse the url and update properties object
        parseURL(url, info);

        // construct list of driverPropertyInfo objects
        List driverProps = new LinkedList();

        if (info.getProperty(BaseDataSource.VDB_VERSION) == null) {
            driverProps.add(new DriverPropertyInfo(BaseDataSource.VDB_VERSION, null));
        }

        if (info.getProperty(BaseDataSource.USER_NAME) == null) {
            driverProps.add(new DriverPropertyInfo(BaseDataSource.USER_NAME, null));
        }

        if (info.getProperty(BaseDataSource.PASSWORD) == null) {
            driverProps.add(new DriverPropertyInfo(BaseDataSource.PASSWORD, null));
        }

        if (info.getProperty(BaseDataSource.LOG_FILE) == null) {
            driverProps.add(new DriverPropertyInfo(BaseDataSource.LOG_FILE, null));
        }

        if (info.getProperty(BaseDataSource.LOG_LEVEL) == null) {
            driverProps.add(new DriverPropertyInfo(BaseDataSource.LOG_LEVEL, null));
        }

        if (info.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP) == null) {
            driverProps.add(new DriverPropertyInfo(ExecutionProperties.PROP_TXN_AUTO_WRAP, null));
        }

        if (info.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE) == null) {
            driverProps.add(new DriverPropertyInfo(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE, null));
        }
        
        if (info.getProperty(ExecutionProperties.RESULT_SET_CACHE_MODE) == null) {
            driverProps.add(new DriverPropertyInfo(ExecutionProperties.RESULT_SET_CACHE_MODE, null));
        }

        if (info.getProperty(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD) == null) {
            driverProps.add(new DriverPropertyInfo(ConnectionProperties.PROP_CLIENT_SESSION_PAYLOAD, null));
        }
        
        if (info.getProperty(CONNECTION.AUTO_FAILOVER) == null) {
            driverProps.add(new DriverPropertyInfo(CONNECTION.AUTO_FAILOVER, null));
        }
        
        if (info.getProperty(ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS) == null) {
            driverProps.add(new DriverPropertyInfo(ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS, null));
        }

        if (info.getProperty(ExecutionProperties.ALLOW_DBL_QUOTED_VARIABLE) == null) {
            driverProps.add(new DriverPropertyInfo(ExecutionProperties.ALLOW_DBL_QUOTED_VARIABLE, null));
        }

        if (info.getProperty(ExecutionProperties.PROP_SQL_OPTIONS) == null) {
            driverProps.add(new DriverPropertyInfo(ExecutionProperties.PROP_SQL_OPTIONS, null));
        }

        // create an array of DriverPropertyInfo objects
        DriverPropertyInfo [] propInfo = new DriverPropertyInfo[driverProps.size()];

        // copy the elements from the list to the array
        return (DriverPropertyInfo[])driverProps.toArray(propInfo);
    }

    /**
     * Get's the driver's major version number. Initially this should be 1.
     * @return major version number of the driver.
     */
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /**
     * Get's the driver's minor version number. Initially this should be 0.
     * @return major version number of the driver.
     */
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    /** 
     * @see com.metamatrix.jdbc.BaseDriver#getDriverName()
     * @since 4.3
     */
    public String getDriverName() {
        return DRIVER_NAME;
    }

}

