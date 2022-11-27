/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

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

import org.teiid.core.TeiidException;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.jdbc.JDBCURL.ConnectionType;
import org.teiid.net.TeiidURL;


/**
 * JDBC Driver class for Teiid Embedded and Teiid Server. This class automatically registers with the
 * {@link DriverManager}
 *
 *  The accepted URL format for the connection
 *  <ul>
 *      <li> Server/socket connection:<b> jdbc:teiid:&lt;vdb-name&gt;@mm[s]://&lt;server-name&gt;:&lt;port&gt;;[user=&lt;user-name&gt;][password=&lt;user-password&gt;][other-properties]*</b>
 *      <li> Embedded  connection:<b> jdbc:teiid:&lt;vdb-name&gt;@&lt;file-path-to-deploy.properties&gt;;[user=&lt;user-name&gt;][password=&lt;user-password&gt;][other-properties]*</b>
 *  </ul>
 *  The user, password properties are needed if the user authentication is turned on. All the "other-properties" are simple name value pairs.
 *  Look at {@link JDBCURL} KNOWN_PROPERTIES for list of known properties allowed.
 */

public class TeiidDriver implements Driver {

    static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
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
    private ConnectionProfile localProfile;

    public static TeiidDriver getInstance() {
        return INSTANCE;
    }

    public ConnectionImpl connect(String url, Properties info) throws SQLException {
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

        ConnectionProfile cp = socketProfile;
        if (conn == ConnectionType.Embedded) {
            if (localProfile == null) {
                try {
                    localProfile = (ConnectionProfile) ReflectionHelper.create(
                            "org.teiid.jdbc.jboss.ModuleLocalProfile", null, //$NON-NLS-1$
                            getClass().getClassLoader());
                } catch (TeiidException e) {
                    throw TeiidSQLException.create(e, JDBCPlugin.Util.gs("module_load_failed"));//$NON-NLS-1$
                }
            }
            cp = localProfile;
        }
        try {
            myConnection = cp.connect(url, info);
        } catch (TeiidSQLException e) {
            logger.log(Level.FINE, "Could not create connection", e); //$NON-NLS-1$
            throw TeiidSQLException.create(e, e.getMessage());
        }

        // logging
        String logMsg = JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess"); //$NON-NLS-1$
        logger.fine(logMsg);

        return myConnection;
    }

    public void setLocalProfile(ConnectionProfile embeddedProfile) {
        this.localProfile = embeddedProfile;
    }

    public void setSocketProfile(ConnectionProfile socketProfile) {
        this.socketProfile = socketProfile;
    }

    /**
     * Returns true if the driver thinks that it can open a connection to the given URL.
     * Expected URL format for server mode is
     * jdbc:teiid::VDB{@literal @}mm://server:port;version=1;user=username;password=password
     *
     * @param url used to establish a connection.
     * @return A boolean value indicating whether the driver understands the subprotocol.
     * @throws SQLException should never occur
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

        for (String property: JDBCURL.KNOWN_PROPERTIES.keySet()) {
            DriverPropertyInfo dpi = new DriverPropertyInfo(property, info.getProperty(property));
            if (property.equals(BaseDataSource.VDB_NAME)) {
                dpi.required = true;
            }
            dpi.description = JDBCPlugin.Util.getString(property + "_desc"); //$NON-NLS-1$
            if (JDBCPlugin.Util.keyExists(property + "_choices")) { //$NON-NLS-1$
                dpi.choices = JDBCPlugin.Util.getString(property + "_choices").split(","); //$NON-NLS-1$ //$NON-NLS-2$
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
     * @param url The URL needed to be parsed.
     * @param info The properties object which is to be updated with properties in the URL.
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
            Enumeration<?> keys = optionalParams.keys();
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

    public Logger getParentLogger() {
        return logger;
    }
}


