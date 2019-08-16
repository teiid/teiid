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

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.XAConnection;

import org.teiid.net.TeiidURL;


/**
 * The Teiid JDBC DataSource implementation class of {@link javax.sql.DataSource} and
 * {@link javax.sql.XADataSource}.
 * <p>
 * The {@link javax.sql.DataSource} interface follows the JavaBean design pattern,
 * meaning the implementation class has <i>properties</i> that are accessed with getter methods
 * and set using setter methods, and where the getter and setter methods follow the JavaBean
 * naming convention (e.g., <code>get</code><i>PropertyName</i><code>() : </code><i>PropertyType</i>
 * and <code>set</code><i>PropertyName</i><code>(</code><i>PropertyType</i><code>) : void</code>).
 *
 * The {@link javax.sql.XADataSource} interface is almost identical to the {@link javax.sql.DataSource}
 * interface, but rather than returning {@link java.sql.Connection} instances, there are methods that
 * return {@link javax.sql.XAConnection} instances that can be used with distributed transactions.
 * <p>
 * The following are the properties for this DataSource:
 * <table summary="properties">
 *   <tr><td><b>Property Name</b></td><td><b>Type</b></td><td><b>Description</b></td></tr>
 *   <tr><td>portNumber       </td><td><code>int   </code></td><td>The port number where a Teiid Server is listening
 *                                                                 for requests.</td></tr>
 *   <tr><td>serverName       </td><td><code>String</code></td><td>The hostname or IP address of the Teiid Server.</td></tr>
 * </table>
 * If "serverName" property is not set then data source will try to create a embedded connection to the Teiid server.
 */
public class TeiidDataSource extends BaseDataSource {

    private static final long serialVersionUID = -5170316154373144878L;

    /**
     * The port number where a server is listening for requests.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>required</i>.
     */
    private int portNumber;

    /**
     * The name of the host where the sServer is running.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>required</i>.
     */
    private String serverName;

    /**
     * Specify whether to make a secure (SSL, mms:) connection or a normal non-SSL mm: connection.
     * the default is to use a non-secure connection.
     * @since 5.0.2
     */
    private boolean secure = false;

    /**
     * Holds a comma delimited list of alternate Server(s):Port(s) that can
     * be used for connection fail-over.
     * @since 5.5
     */
    private String alternateServers;

    /**
     * The auto failover mode for calls made to the query engine.  If true query engine calls that fail will
     * allow the connection to choose another process.
     */
    private String autoFailover;

    /**
     * when "true", in the "embedded" scenario, authentication is information is read in pass though manner.
     */
    private boolean passthroughAuthentication = false;

    /**
     * Name of the jass configuration to use from the -Djava.security.auth.login.config=login.conf property
     */
    private String jaasName;

    /**
     * Name of Kerberos KDC service principle name
     */
    private String kerberosServicePrincipleName;
    /**
     * If not using ssl determines whether requests with the associated command payload should be encrypted
     */
    private boolean encryptRequests;

    private final TeiidDriver driver;

    public TeiidDataSource() {
        this.driver = new TeiidDriver();
    }

    TeiidDataSource(TeiidDriver driver) {
        this.driver = driver;
    }

    // --------------------------------------------------------------------------------------------
    //                             H E L P E R   M E T H O D S
    // --------------------------------------------------------------------------------------------

    protected Properties buildProperties(final String userName, final String password) {
        Properties props = super.buildProperties(userName, password);

        if (this.getAutoFailover() != null) {
            props.setProperty(TeiidURL.CONNECTION.AUTO_FAILOVER, this.getAutoFailover());
        }

        if (this.encryptRequests) {
            props.setProperty(TeiidURL.CONNECTION.ENCRYPT_REQUESTS, Boolean.TRUE.toString());
        }

        if (getLoginTimeout() > 0) {
            props.setProperty(TeiidURL.CONNECTION.LOGIN_TIMEOUT, String.valueOf(getLoginTimeout()));
        }

        if (getJaasName() != null) {
            props.setProperty(TeiidURL.CONNECTION.JAAS_NAME, getJaasName());
        }
        if (getKerberosServicePrincipleName() != null) {
            props.setProperty(TeiidURL.CONNECTION.KERBEROS_SERVICE_PRINCIPLE_NAME, getKerberosServicePrincipleName());
        }

        return props;
    }

    protected String buildServerURL() throws TeiidSQLException {
        if (serverName == null) {
            return null;
        }

        if ( this.alternateServers == null || this.alternateServers.length() == 0) {
            // Format:  "mm://server:port"
            return new TeiidURL(this.serverName, this.portNumber, this.secure).getAppServerURL();
        }

        // Format: "mm://server1:port,server2:port,..."
        String serverURL = this.secure ? TeiidURL.SECURE_PROTOCOL : TeiidURL.DEFAULT_PROTOCOL;

        if (this.serverName.indexOf(':') != -1 && !this.serverName.startsWith("[")) { //$NON-NLS-1$
            serverURL += "[" + this.serverName + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            serverURL += this.serverName;
        }

        serverURL += TeiidURL.COLON_DELIMITER + this.portNumber;

        //add in the port number if not specified

        String[] as = this.alternateServers.split( TeiidURL.COMMA_DELIMITER);

        for ( int i = 0; i < as.length; i++ ) {
            String server = as[i].trim();
            //ipv6 without port
            if (server.startsWith("[") && server.endsWith("]")) { //$NON-NLS-1$ //$NON-NLS-2$
                String msg = reasonWhyInvalidServerName(server.substring(1, server.length() - 1));
                if (msg != null) {
                    throw createConnectionError(JDBCPlugin.Util.getString("MMDataSource.alternateServer_is_invalid", msg)); //$NON-NLS-1$
                }
                serverURL += (TeiidURL.COMMA_DELIMITER +as[i] + TeiidURL.COLON_DELIMITER + this.portNumber);
            } else {
                String[] serverParts = server.split(TeiidURL.COLON_DELIMITER, 2);
                String msg = reasonWhyInvalidServerName(serverParts[0]);
                if (msg != null) {
                    throw createConnectionError(JDBCPlugin.Util.getString("MMDataSource.alternateServer_is_invalid", msg)); //$NON-NLS-1$
                }
                serverURL += (TeiidURL.COMMA_DELIMITER + serverParts[0] + TeiidURL.COLON_DELIMITER);
                if ( serverParts.length > 1 ) {
                    try {
                        TeiidURL.validatePort(serverParts[1]);
                    } catch (MalformedURLException e) {
                        throw createConnectionError(JDBCPlugin.Util.getString("MMDataSource.alternateServer_is_invalid", e.getMessage())); //$NON-NLS-1$
                    }

                    serverURL += serverParts[1];
                } else {
                    serverURL += this.portNumber;
                }
            }
        }

        try {
            return new TeiidURL(serverURL).getAppServerURL();
        } catch (MalformedURLException e) {
            throw TeiidSQLException.create(e);
        }
    }

    protected JDBCURL buildURL() throws TeiidSQLException {
        return new JDBCURL(this.getDatabaseName(), buildServerURL(), buildProperties(getUser(), getPassword()));
    }

    protected void validateProperties( final String userName, final String password) throws java.sql.SQLException {
        super.validateProperties(userName, password);

        String reason = reasonWhyInvalidPortNumber(this.portNumber);
        if ( reason != null ) {
            throw createConnectionError(reason);
        }

        reason = reasonWhyInvalidServerName(this.serverName);
        if ( reason != null ) {
            throw createConnectionError(reason);
        }
    }

    private TeiidSQLException createConnectionError(String reason) {
        String msg = JDBCPlugin.Util.getString("MMDataSource.Err_connecting", reason); //$NON-NLS-1$
        return new TeiidSQLException(msg);
    }

    // --------------------------------------------------------------------------------------------
    //                        D A T A S O U R C E   M E T H O D S
    // --------------------------------------------------------------------------------------------

    /**
     * Attempt to establish a database connection.
     * @return a Connection to the database
     * @throws java.sql.SQLException if a database-access error occurs
     * @see javax.sql.DataSource#getConnection()
     */
    public Connection getConnection() throws java.sql.SQLException {
        return getConnection(null,null);
    }

    /**
     * Attempt to establish a database connection.
     * @param userName the database user on whose behalf the Connection is being made
     * @param password the user's password
     * @return a Connection to the database
     * @throws java.sql.SQLException if a database-access error occurs
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    public Connection getConnection(String userName, String password) throws java.sql.SQLException {

        // check if this is embedded connection
        if (getServerName() == null) {
            super.validateProperties(userName, password);
            final Properties props = buildEmbeddedProperties(userName, password);
            String url = new JDBCURL(getDatabaseName(), null, null).getJDBCURL();
            return driver.connect(url, props);
        }

        // if not proceed with socket connection.
        validateProperties(userName,password);
        final Properties props = buildProperties(userName, password);
        return driver.connect(new JDBCURL(this.getDatabaseName(), buildServerURL(), null).getJDBCURL(), props);
    }

    private Properties buildEmbeddedProperties(final String userName, final String password) {
        Properties props = buildProperties(userName, password);
        props.setProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, Boolean.toString(this.passthroughAuthentication));
        return props;
    }

   /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        try {
            return buildURL().getJDBCURL();
        } catch (TeiidSQLException e) {
            return e.getMessage();
        }
    }

    // --------------------------------------------------------------------------------------------
    //                        P R O P E R T Y   M E T H O D S
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the port number.
     * @return the port number
     */
    public int getPortNumber() {
        return portNumber;
    }

    /**
     * Returns the name of the server.
     * @return the name of the server
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Returns a flag indicating whether to create a secure connection or not.
     * @return True if using secure mms: protocol, false for normal mm: protocol.
     * @since 5.0.2
     */
    public boolean isSecure() {
        return this.secure;
    }
    /**
     * Same as "isSecure". Required by the reflection login in connection pools to identify the type
     * @return
     */
    public boolean getSecure() {
        return this.secure;
    }

    /**
     * Returns a string containing a comma delimited list of alternate
     * server(s).
     *
     * The list will be in the form of server2[:port2][,server3[:port3]].  If no
     * alternate servers have been defined <code>null</code> is returned.
     * @return A comma delimited list of server:port or <code>null</code> If
     * no alternate servers are defined.
     * @since 5.5
     */
    public String getAlternateServers() {
        if ( this.alternateServers != null && this.alternateServers.length() < 1 ) {
            return null;
        }
        return this.alternateServers;
    }

    /**
     * Sets the portNumber.
     * @param portNumber The portNumber to set
     */
    public void setPortNumber(final int portNumber) {
        this.portNumber = portNumber;
    }

    /**
     * Sets the serverName.
     * @param serverName The serverName to set
     */
    public void setServerName(final String serverName) {
        this.serverName = serverName;
    }

    /**
     * Sets the secure flag to use mms: protocol instead of the default mm: protocol.
     * @param secure True to use mms:
     * @since 5.0.2
     */
    public void setSecure(final boolean secure) {
        this.secure = secure;
    }

    /**
     * Sets a list of alternate server(s) that can be used for
     * connection fail-over.
     *
     * The form of the list should be server2[:port2][,server3:[port3][,...]].
     *
     * If ":port" is omitted, the port defined by <code>portNumber</code> is used.
     *
     * If <code>servers</code> is empty or <code>null</code>, the value of
     * <code>alternateServers</code> is cleared.
     * @param servers A comma delimited list of alternate
     * Server(s):Port(s) to use for connection fail-over. If blank or
     * <code>null</code>, the list is cleared.
     * @since 5.5
     */
    public void setAlternateServers(final String servers) {
        this.alternateServers = servers;
        if ( this.alternateServers != null && this.alternateServers.length() < 1 ) {
            this.alternateServers = null;
        }
    }


    // --------------------------------------------------------------------------------------------
    //                  V A L I D A T I O N   M E T H O D S
    // --------------------------------------------------------------------------------------------

    /**
     * Return the reason why the supplied port number may be invalid, or null
     * if it is considered valid.
     * @param portNumber a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setPortNumber(int)
     */
    public static String reasonWhyInvalidPortNumber( final int portNumber) {
        return TeiidURL.validatePort(portNumber);
    }

    /**
     * Return the reason why the supplied server name may be invalid, or null
     * if it is considered valid.
     * @param serverName a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setServerName(String)
     * */
    public static String reasonWhyInvalidServerName( final String serverName ) {
        if ( serverName == null || serverName.trim().length() == 0 ) {
            return JDBCPlugin.Util.getString("MMDataSource.Server_name_required"); //$NON-NLS-1$
        }
        return null;
    }

    /**
     * The reason why "socketsPerVM" is invalid.
     * @param socketsPerVM property
     * @return reason
     */
    public static String reasonWhyInvalidSocketsPerVM(final String socketsPerVM) {
        if (socketsPerVM != null) {
            int value = -1;
            try {
                value = Integer.parseInt(socketsPerVM);
            } catch (Exception e) {
            }

            if (value <= 0) {
                return JDBCPlugin.Util.getString("MMDataSource.Sockets_per_vm_invalid"); //$NON-NLS-1$
            }
        }
        return null;
    }


    /**
     * The reason why "stickyConnections" is invalid.
     * @param stickyConnections property
     * @return reason
     */
    public static String reasonWhyInvalidStickyConnections(final String stickyConnections) {
        if (stickyConnections != null) {
            if ((! stickyConnections.equalsIgnoreCase("true")) &&    //$NON-NLS-1$
                (! stickyConnections.equalsIgnoreCase("false"))) {   //$NON-NLS-1$
                return JDBCPlugin.Util.getString("MMDataSource.Sticky_connections_invalid"); //$NON-NLS-1$
            }
        }
        return null;
    }

    /**
     * @return Returns the transparentFailover.
     */
    public String getAutoFailover() {
        return this.autoFailover;
    }

    /**
     * @param autoFailover The transparentFailover to set.
     */
    public void setAutoFailover(String autoFailover) {
        this.autoFailover = autoFailover;
    }

    /**
     * When true, this connection uses the passed in security domain to do the authentication.
     * @return
     */
    public boolean isPassthroughAuthentication() {
        return passthroughAuthentication;
    }

    /**
     * Same as "isPassthroughAuthentication". Required by the reflection login in connection pools to identify the type
     * @return
     */
    public boolean getPassthroughAuthentication() {
        return passthroughAuthentication;
    }

    /**
     * When set to true, the connection uses the passed in security domain to do the authentication.
     * @since 7.1
     */
    public void setPassthroughAuthentication(final boolean passthroughAuthentication) {
        this.passthroughAuthentication = passthroughAuthentication;
    }

    /**
     * Application name from JAAS Login Config file
     * @since 7.6
     * @return
     */
    public String getJaasName() {
        return jaasName;
    }

    /**
     * Application name from JAAS Login Config file
     * @since 7.6
     */
    public void setJaasName(String jaasApplicationName) {
        this.jaasName = jaasApplicationName;
    }

    /**
     * Kerberos KDC service principle name
     * @since 7.6
     * @return
     */
    public String getKerberosServicePrincipleName() {
        return kerberosServicePrincipleName;
    }

    /**
     * Kerberos KDC service principle name
     * @since 7.6
     */
    public void setKerberosServicePrincipleName(String kerberosServerName) {
        this.kerberosServicePrincipleName = kerberosServerName;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return TeiidDriver.logger;
    }

    public void setEncryptRequests(boolean encryptRequests) {
        this.encryptRequests = encryptRequests;
    }

    public boolean isEncryptRequests() {
        return encryptRequests;
    }

    public boolean getEncryptRequests() {
        return encryptRequests;
    }

    @Deprecated
    public boolean isLoadBalance() {
        return false;
    }

    @Deprecated
    public boolean getLoadBalance() {
        return false;
    }

    @Deprecated
    public void setLoadBalance(boolean loadBalance) {

    }

    /**
     * Attempt to establish a database connection that can be used with distributed transactions.
     * @param userName the database user on whose behalf the XAConnection is being made
     * @param password the user's password
     * @return an XAConnection to the database
     * @throws java.sql.SQLException if a database-access error occurs
     * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
     */
    public XAConnection getXAConnection(final String userName, final String password) throws java.sql.SQLException {
        XAConnectionImpl result = new XAConnectionImpl((ConnectionImpl) getConnection(userName, password));
        return result;
    }

}

