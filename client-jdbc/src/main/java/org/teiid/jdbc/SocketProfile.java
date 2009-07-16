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

package org.teiid.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.jdbc.BaseDataSource;
import com.metamatrix.jdbc.JDBCPlugin;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.MMSQLException;
import com.metamatrix.jdbc.util.MMJDBCURL;

/**
 * <p> The java.sql.DriverManager class uses this class to connect to Teiid Server or Teiid Embedded.
 * The TeiidDriver class has a static initializer, which
 * is used to instantiate and register itself with java.sql.DriverManager. The
 * DriverManager's <code>getConnection</code> method calls <code>connect</code>
 * method on available registered drivers. </p>
 */

final class SocketProfile {
	
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    
    /**
     *  Suports JDBC URLS of format
     *  - jdbc:teiid:BQT@mm://localhost:####;version=1
     *  - jdbc:teiid:BQT@mms://localhost:####;version=1
     *  - jdbc:teiid:BQT@mm(s)://host1:####,host2:####,host3:####;version=1
     */
    
    // This host/port pattern allows just a . or a - to be in the host part.
    static final String HOST_PORT_PATTERN = "[\\p{Alnum}\\.\\-]+:\\d+"; //$NON-NLS-1$
    static final String URL_PATTERN = "jdbc:(metamatrix|teiid):(\\w+)@((mm[s]?://"+HOST_PORT_PATTERN+"(,"+HOST_PORT_PATTERN+")*)[;]?){1}((.*)*)"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    static Pattern urlPattern = Pattern.compile(URL_PATTERN);
    
    /**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection to the server.
     */
    public static Connection connect(String url, Properties info) throws SQLException {

        MMConnection myConnection = null;
        // create a properties obj if it is null
        if(info == null) {
            info = new Properties();
        } else {
            info = PropertiesUtils.clone(info);
        }

        try {
            // parse the URL to add it's properties to properties object
            parseURL(url, info);

            myConnection = createConnection(url, info);
        } catch (MetaMatrixCoreException e) {
            logger.log(Level.SEVERE, "Could not create connection", e); //$NON-NLS-1$
            throw MMSQLException.create(e, e.getMessage());
        }

        // logging
        String logMsg = JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess"); //$NON-NLS-1$
        logger.fine(logMsg);

        return myConnection;
    }

    static MMConnection createConnection(String url, Properties info)
        throws ConnectionException, CommunicationException {

        ServerConnection serverConn = SocketServerConnectionFactory.getInstance().createConnection(info);

        // construct a MMConnection object.
        MMConnection connection = new MMConnection(serverConn, info, url);
        return connection;
    }

    /**
     * This method parses the URL and adds properties to the the properties object.
     * These include required and any optional properties specified in the URL.
     * @param The URL needed to be parsed.
     * @param The properties object which is to be updated with properties in the URL.
     * @throws SQLException if the URL is not in the expected format.
     */
    protected static void parseURL(String url, Properties info) throws SQLException {
        if(url == null) {
            String msg = JDBCPlugin.Util.getString("MMDriver.urlFormat"); //$NON-NLS-1$
            throw new MMSQLException(msg);
        }
        try {
            MMJDBCURL jdbcURL = new MMJDBCURL(url);
            info.setProperty(BaseDataSource.VDB_NAME, jdbcURL.getVDBName());
            info.setProperty(MMURL.CONNECTION.SERVER_URL, jdbcURL.getConnectionURL());
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
            // add the property only if it is new because they could have
            // already been specified either through url or otherwise.
            if(!info.containsKey(BaseDataSource.VDB_VERSION) && jdbcURL.getVDBVersion() != null) {
                info.setProperty(BaseDataSource.VDB_VERSION, jdbcURL.getVDBVersion());
            }
            if(!info.containsKey(BaseDataSource.APP_NAME)) {
                info.setProperty(BaseDataSource.APP_NAME, BaseDataSource.DEFAULT_APP_NAME);
            }

        } catch(IllegalArgumentException iae) {
            throw new MMSQLException(JDBCPlugin.Util.getString("MMDriver.urlFormat")); //$NON-NLS-1$
        }  
    }
    

    public static boolean acceptsURL(String url) {
        Matcher m = urlPattern.matcher(url);
        return m.matches();
    }
}


