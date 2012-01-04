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
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.JDBCURL.ConnectionType;
import org.teiid.net.TeiidURL;


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
 *  Look at {@link JDBCURL} KNOWN_PROPERTIES for list of known properties allowed.
 */

public class TeiidDriver implements Driver {
	
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
	static final String DRIVER_NAME = "Teiid JDBC Driver"; //$NON-NLS-1$
	
    private static TeiidDriver INSTANCE = new TeiidDriver();
        
    static {
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch(SQLException e) {
            // Logging
            String logMsg = JDBCPlugin.Util.getString("MMDriver.Err_registering", e.getMessage()); //$NON-NLS-1$
            logger.log(Level.SEVERE, logMsg);
        }
    }
    
    private ConnectionProfile socketProfile = new SocketProfile();
    private ConnectionProfile embeddedProfile = new EmbeddedProfile();

    public static TeiidDriver getInstance() {
        return INSTANCE;
    }
    
    /**
     * Should be a singleton and only constructed in {@link #getInstance}.
     */
    public TeiidDriver() {
        // this is not singleton, if you want singleton make this private.
    }

    public Connection connect(String url, Properties info) throws SQLException {
    	ConnectionType conn = JDBCURL.acceptsUrl(url);
    	if (conn == null) {
    		return null;
    	}
        if(info == null) {
        	// create a properties obj if it is null
            info = new Properties();
        } else {
        	//don't modify the original
            info = PropertiesUtils.clone(info);
        }
        parseURL(url, info);
        
        ConnectionImpl myConnection = null;

        try {
        	if (conn == ConnectionType.Embedded) {
        		myConnection = embeddedProfile.connect(url, info);
        	} else { 
        		myConnection = socketProfile.connect(url, info);
        	}
        } catch (TeiidSQLException e) {
            logger.log(Level.SEVERE, "Could not create connection", e); //$NON-NLS-1$
            throw TeiidSQLException.create(e, e.getMessage());
        }

        // logging
        String logMsg = JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess"); //$NON-NLS-1$
        logger.fine(logMsg);
        
		return myConnection;
    }
    
    public void setEmbeddedProfile(ConnectionProfile embeddedProfile) {
		this.embeddedProfile = embeddedProfile;
	}
    
    public void setSocketProfile(ConnectionProfile socketProfile) {
		this.socketProfile = socketProfile;
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
    	return JDBCURL.acceptsUrl(url) != null;
    }

    public int getMajorVersion() {
        return ApplicationInfo.getInstance().getMajorReleaseVersion();
    }

    public int getMinorVersion() {
        return ApplicationInfo.getInstance().getMinorReleaseVersion();
    }

    public String getDriverName() {
        return DRIVER_NAME;
    }
    
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if(info == null) {
            info = new Properties();
        } else {
        	info = PropertiesUtils.clone(info);
        }

        // construct list of driverPropertyInfo objects
        List<DriverPropertyInfo> driverProps = new LinkedList<DriverPropertyInfo>();

        parseURL(url, info);

        for (String property: JDBCURL.KNOWN_PROPERTIES) {
        	DriverPropertyInfo dpi = new DriverPropertyInfo(property, info.getProperty(property));
        	if (property.equals(BaseDataSource.VDB_NAME)) {
        		dpi.required = true;
        	}
        	driverProps.add(dpi);
        }
        
        // create an array of DriverPropertyInfo objects
        DriverPropertyInfo [] propInfo = new DriverPropertyInfo[driverProps.size()];

        // copy the elements from the list to the array
        return driverProps.toArray(propInfo);
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
            throw new TeiidSQLException(msg);
        }
        try {
            JDBCURL jdbcURL = new JDBCURL(url);
            info.setProperty(BaseDataSource.VDB_NAME, jdbcURL.getVDBName());
            if (jdbcURL.getConnectionURL() != null) {
            	info.setProperty(TeiidURL.CONNECTION.SERVER_URL, jdbcURL.getConnectionURL());
            }
            Properties optionalParams = jdbcURL.getProperties();
            JDBCURL.normalizeProperties(info);
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
            throw new TeiidSQLException(JDBCPlugin.Util.getString("MMDriver.urlFormat")); //$NON-NLS-1$
        }  
    }
    
    /**
     * This method returns true if the driver passes jdbc compliance tests.
     * @return true if the driver is jdbc complaint, else false.
     */
    public boolean jdbcCompliant() {
        return false;
    }
}


