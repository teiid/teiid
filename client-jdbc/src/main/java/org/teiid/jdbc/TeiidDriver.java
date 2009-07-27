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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.jdbc.JDBCPlugin;
import com.metamatrix.jdbc.util.MMJDBCURL;

/**
 * JDBC Driver class for Teiid Embedded and Teiid Server. This class automatically registers with the 
 * {@link DriverManager}
 * 
 *  The accepted URL format for the connection
 *  <ul>
 *  	<li> Server/socket connection:<b> jdbc:teiid:&lt;vdb-name&gt;@mm[s]://&lt;server-name&gt;:&lt;port&gt;;[user=&lt;user-name&gt;][password=&lt;user-password&gt;][other-properties]*</b>
 *  	<li> Embedded  connection:<b> jdbc:teiid:&lt;vdb-name&gt;@&lt;file-path-to-deploy.properties&gt;;[user=&lt;user-name&gt;][password=&lt;user-password&gt;][other-properties]*</b>
 *  </ul>
 *  The user, password properties are needed if the user authentication is turned on. All the "other-properties" are simple name value pairs.
 *  Look at {@link MMJDBCURL} KNOWN_PROPERTIES for list of known properties allowed.
 */

public class TeiidDriver implements Driver {
	
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
	static final String DRIVER_NAME = "Teiid JDBC Driver"; //$NON-NLS-1$
	
    private static TeiidDriver INSTANCE = new TeiidDriver();
        
    // Static initializer
    static {
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch(SQLException e) {
            // Logging
            String logMsg = JDBCPlugin.Util.getString("MMDriver.Err_registering", e.getMessage()); //$NON-NLS-1$
            logger.log(Level.SEVERE, logMsg);
        }
    }

    public static TeiidDriver getInstance() {
        return INSTANCE;
    }
    
    /**
     * Should be a singleton and only constructed in {@link #getInstance}.
     */
    public TeiidDriver() {
        // this is not singleton, if you want singleton make this private.
    }

    /**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection to the MetaMatrix server.
     */
    public Connection connect(String url, Properties info) throws SQLException {

    	if (EmbeddedProfile.acceptsURL(url)) {
    		return EmbeddedProfile.connect(url, info);
    	}
    	else if (SocketProfile.acceptsURL(url)) {
    		return SocketProfile.connect(url, info);
    	}
    	return null;
    }
    
    /**
     * Returns true if the driver thinks that it can open a connection to the given URL.
     * Expected URL format for server mode is
     * jdbc:teiid::VDB@mm://server:port;version=1;user=username;password=password
     * 
     * @param The URL used to establish a connection.
     * @return A boolean value indicating whether the driver understands the subprotocol.
     * @throws SQLException, should never occur
     */
    public boolean acceptsURL(String url) throws SQLException {
    	return EmbeddedProfile.acceptsURL(url) || SocketProfile.acceptsURL(url);
    }

    /**
     * Get's the driver's major version number. Initially this should be 1.
     * @return major version number of the driver.
     */
    public int getMajorVersion() {
        return ApplicationInfo.getInstance().getMajorReleaseVersion();
    }

    /**
     * Get's the driver's minor version number. Initially this should be 0.
     * @return major version number of the driver.
     */
    public int getMinorVersion() {
        return ApplicationInfo.getInstance().getMinorReleaseVersion();
    }

    public String getDriverName() {
        return DRIVER_NAME;
    }
    
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if(info == null) {
            info = new Properties();
        }

        // construct list of driverPropertyInfo objects
        List<DriverPropertyInfo> driverProps = new LinkedList<DriverPropertyInfo>();

        for (String property: MMJDBCURL.KNOWN_PROPERTIES) {
        	DriverPropertyInfo dpi = new DriverPropertyInfo(property, info.getProperty(property));
        	driverProps.add(dpi);
        }
                
        // create an array of DriverPropertyInfo objects
        DriverPropertyInfo [] propInfo = new DriverPropertyInfo[driverProps.size()];

        // copy the elements from the list to the array
        return driverProps.toArray(propInfo);
    }    
    
    /**
     * This method returns true if the driver passes jdbc compliance tests.
     * @return true if the driver is jdbc complaint, else false.
     */
    public boolean jdbcCompliant() {
        return false;
    }
}


