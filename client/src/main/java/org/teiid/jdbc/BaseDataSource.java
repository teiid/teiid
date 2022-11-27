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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.teiid.client.RequestMessage;
import org.teiid.jdbc.ExecutionProperties.Values;
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
 */
public abstract class BaseDataSource extends WrapperImpl implements javax.sql.DataSource, XADataSource, ConnectionPoolDataSource, java.io.Serializable {
    public static final String DEFAULT_APP_NAME = "JDBC"; //$NON-NLS-1$

    // constant indicating Virtual database name
    public static final String VDB_NAME = TeiidURL.JDBC.VDB_NAME;
    // constant indicating Virtual database version
    public static final String VDB_VERSION = TeiidURL.JDBC.VDB_VERSION;
    // constant for vdb version part of serverURL
    public static final String VERSION = TeiidURL.JDBC.VERSION;
    // name of the application which is obtaining connection
    public static final String APP_NAME = TeiidURL.CONNECTION.APP_NAME;
    // constant for username part of url
    public static final String USER_NAME = TeiidURL.CONNECTION.USER_NAME;
    // constant for password part of url
    public static final String PASSWORD = TeiidURL.CONNECTION.PASSWORD;

    protected static final int DEFAULT_TIMEOUT = 0;
    protected static final int DEFAULT_LOG_LEVEL = 0;

    /**
     * The name of the virtual database on a particular Teiid Server.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>required</i>.
     */
    private String databaseName;

    /**
     * The logical name for the underlying <code>XADataSource</code> or
     * <code>ConnectionPoolDataSource</code>;
     * used only when pooling connections or distributed transactions are implemented.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>optional</i>.
     */
    private String dataSourceName;

    /**
     * The description of this data source.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>optional</i>.
     */
    private String description;


    /**
     * The user's name.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>required</i>.
     */
    private String user;

    /**
     * The user's password.
     * This property name is one of the standard property names defined by the JDBC 2.0 specification,
     * and is <i>required</i>.
     */
    private String password;

    /**
     * The version number of the virtual database to which a connection is to be established.
     * This property is <i>optional</i>; if not specified, the assumption is that the latest version
     * on the Teiid Server is to be used.
     */
    private String databaseVersion;

    /**
     * The name of the application.  Supplying this property may allow an administrator of a
     * Teiid Server to better identify individual connections and usage patterns.
     * This property is <i>optional</i>.
     */
    private String applicationName;

    /** Support partial results mode or not.*/
    private String partialResultsMode;

    /** Default fetch size, &lt;= 0 indicates not set. */
    private int fetchSize = BaseDataSource.DEFAULT_FETCH_SIZE;

    /** Whether to use result set cache if it available **/
    private String resultSetCacheMode;

    /**
     * The number of milliseconds before timing out.
     * This property is <i>optional</i> and defaults to "0" (meaning no time out).
     */
    private int loginTimeout;

    private String showPlan;

    private boolean noExec;

    private String disableLocalTxn;

    private String transactionAutoWrap;

    private boolean ansiQuotedIdentifiers = true;

    private int queryTimeout;

    private boolean useJDBC4ColumnNameAndLabelSemantics = true;

    /**
     * Reference to the logWriter, which is transient and is therefore not serialized with the DataSource.
     */
    private transient PrintWriter logWriter;
    public static final String JDBC = "jdbc:"; //$NON-NLS-1$
    // Default execution property constants
    protected static final int DEFAULT_FETCH_SIZE = RequestMessage.DEFAULT_FETCH_SIZE;
    protected static final String DEFAULT_PARTIAL_RESULTS_MODE = "FALSE"; //$NON-NLS-1$
    protected static final String DEFAULT_RESULT_SET_CACHE_MODE = "FALSE"; //$NON-NLS-1$

    /**
     * Transaction auto wrap constant - never wrap a command execution in a transaction
     * and allow multi-source updates to occur outside of a transaction.
     */
    public static final String TXN_WRAP_OFF = Values.TXN_WRAP_OFF;

    /**
     * Transaction auto wrap constant - always wrap every non-transactional command
     * execution in a transaction.
     */
    public static final String TXN_WRAP_ON = Values.TXN_WRAP_ON;

    /**
     * Transaction auto wrap constant - checks if a command
     * requires a transaction and will be automatically wrap it.
     */
    public static final String TXN_WRAP_AUTO = Values.TXN_WRAP_DETECT;

    /**
     * String to hold additional properties that are not represented with an explicit getter/setter
     */
    private String additionalProperties;

    /**
     * Constructor for MMDataSource.
     */
    public BaseDataSource() {
        this.loginTimeout = DEFAULT_TIMEOUT;
    }

    // --------------------------------------------------------------------------------------------
    //                             H E L P E R   M E T H O D S
    // --------------------------------------------------------------------------------------------

    protected Properties buildProperties(final String userName, final String password) {
        Properties props = new Properties();
        props.setProperty(BaseDataSource.VDB_NAME,this.getDatabaseName());

        if ( this.getDatabaseVersion() != null && this.getDatabaseVersion().trim().length() != 0  ) {
            props.setProperty(BaseDataSource.VDB_VERSION,this.getDatabaseVersion());
        }

        if ( userName != null && userName.trim().length() != 0) {
            props.setProperty(BaseDataSource.USER_NAME, userName);
        } else if ( this.getUser() != null && this.getUser().trim().length() != 0) {
            props.setProperty(BaseDataSource.USER_NAME, this.getUser());
        }

        if ( password != null && password.trim().length() != 0) {
            props.setProperty(BaseDataSource.PASSWORD, password);
        } else if ( this.getPassword() != null && this.getPassword().trim().length() != 0) {
            props.setProperty(BaseDataSource.PASSWORD, this.getPassword());
        }

        if ( this.getApplicationName() != null && this.getApplicationName().trim().length() != 0 ) {
            props.setProperty(BaseDataSource.APP_NAME,this.getApplicationName());
        }

        if (this.getPartialResultsMode() != null && this.getPartialResultsMode().trim().length() != 0) {
            props.setProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE, this.getPartialResultsMode());
        }

        if(this.getFetchSize() > 0) {
            props.setProperty(ExecutionProperties.PROP_FETCH_SIZE, String.valueOf(this.getFetchSize()));
        }

        if (this.getQueryTimeout() > 0) {
            props.setProperty(ExecutionProperties.QUERYTIMEOUT, String.valueOf(this.getQueryTimeout()));
        }

        if (this.getResultSetCacheMode() != null && this.getResultSetCacheMode().trim().length() != 0) {
            props.setProperty(ExecutionProperties.RESULT_SET_CACHE_MODE, this.getResultSetCacheMode());
        }

        if (this.getShowPlan() != null) {
            props.setProperty(ExecutionProperties.SQL_OPTION_SHOWPLAN, this.getShowPlan());
        }

        if (this.isNoExec()) {
            props.setProperty(ExecutionProperties.NOEXEC, String.valueOf(this.isNoExec()));
        }

        if ( this.getAutoCommitTxn() != null && this.getAutoCommitTxn().trim().length() != 0   ) {
            props.setProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP, this.getAutoCommitTxn());
        }

        if (this.getDisableLocalTxn() != null) {
            props.setProperty(ExecutionProperties.DISABLE_LOCAL_TRANSACTIONS, this.getDisableLocalTxn());
        }

        if (!this.getUseJDBC4ColumnNameAndLabelSemantics()) {
            props.setProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS, Boolean.FALSE.toString());
        }

        if (this.additionalProperties != null) {
            JDBCURL.parseConnectionProperties(this.additionalProperties, props);
        }

        return props;
    }

    protected void validateProperties( final String userName, final String password) throws java.sql.SQLException {
        String reason = reasonWhyInvalidApplicationName(this.applicationName);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidDatabaseName(this.databaseName);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidDatabaseVersion(this.databaseVersion);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidDataSourceName(this.dataSourceName);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidDescription(this.description);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        final String pwd = password != null ? password : getPassword();
        reason = reasonWhyInvalidPassword(pwd);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidPartialResultsMode(this.partialResultsMode);
        if (reason != null) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidFetchSize(this.fetchSize);
        if (reason != null) {
            throw new SQLException(reason);
        }

        final String user = userName != null ? userName : getUser();
        reason = reasonWhyInvalidUser(user);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        reason = reasonWhyInvalidTransactionAutoWrap(this.transactionAutoWrap);
        if ( reason != null ) {
            throw new SQLException(reason);
        }

        if (this.queryTimeout < 0) {
            throw new TeiidSQLException(JDBCPlugin.Util.getString("MMStatement.Bad_timeout_value")); //$NON-NLS-1$
        }


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
     * @see javax.sql.XADataSource#getXAConnection()
     */
    public XAConnection getXAConnection() throws SQLException {
        return getXAConnection(null,null);
    }

    public PooledConnection getPooledConnection() throws SQLException {
        return getPooledConnection(null, null);
    }

    public PooledConnection getPooledConnection(final String userName, final String password)
            throws SQLException {
        return getXAConnection(userName, password);
    }

    // --------------------------------------------------------------------------------------------
    //                        P R O P E R T Y   M E T H O D S
    // --------------------------------------------------------------------------------------------

    public String getDisableLocalTxn() {
        return disableLocalTxn;
    }

    public void setDisableLocalTxn(String disableLocalTxn) {
        this.disableLocalTxn = disableLocalTxn;
    }

    @Override
    public PrintWriter getLogWriter() throws java.sql.SQLException{
        return this.logWriter;
    }

    @Override
    public int getLoginTimeout() {
        return this.loginTimeout;
    }

    @Override
    public void setLogWriter( final PrintWriter writer) throws java.sql.SQLException{
        this.logWriter = writer;
    }

    @Override
    public void setLoginTimeout( final int timeOut) throws java.sql.SQLException {
        this.loginTimeout = timeOut;
    }

    /**
     * Returns the name of the application.  Supplying this property may allow an administrator of a
     * Teiid Server to better identify individual connections and usage patterns.
     * This property is <i>optional</i>.
     * @return String the application name; may be null or zero-length
     */
    public String getApplicationName() {
        return applicationName!=null?applicationName:DEFAULT_APP_NAME;
    }

    /**
     * Returns the name of the virtual database on a particular Teiid Server.
     * @return String
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Returns the databaseVersion.
     * @return String
     */
    public String getDatabaseVersion() {
        return databaseVersion;
    }

    /**
     * Returns the logical name for the underlying <code>XADataSource</code> or
     * <code>ConnectionPoolDataSource</code>;
     * used only when pooling connections or distributed transactions are implemented.
     * @return the logical name for the underlying data source; may be null
     */
    public String getDataSourceName() {
        return dataSourceName;
    }

    /**
     * Returns the description of this data source.
     * @return the description; may be null
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the user.
     * @return the name of the user for this data source
     */
    public String getUser() {
        return user;
    }

    /**
     * Returns the password.
     * @return the password for this data source.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the name of the application.  Supplying this property may allow an administrator of a
     * Teiid Server to better identify individual connections and usage patterns.
     * This property is <i>optional</i>.
     * @param applicationName The applicationName to set
     */
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }

    /**
     * Sets the name of the virtual database on a particular Teiid Server.
     * @param databaseName The name of the virtual database
     */
    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Sets the databaseVersion.
     * @param databaseVersion The version of the virtual database
     */
    public void setDatabaseVersion(final String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    /**
     * Sets the logical name for the underlying <code>XADataSource</code> or
     * <code>ConnectionPoolDataSource</code>;
     * used only when pooling connections or distributed transactions are implemented.
     * @param dataSourceName The dataSourceName for this data source; may be null
     */
    public void setDataSourceName(final String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    /**
     * Sets the user.
     * @param user The user to set
     */
    public void setUser(final String user) {
        this.user = user;
    }

    /**
     * Sets the password.
     * @param password The password for this data source
     */
    public void setPassword(final String password) {
        this.password = password;
    }

    /**
     * Sets the description of this data source.
     * @param description The description for this data source; may be null
     */
    public void setDescription(final String description) {
        this.description = description;
    }

    public void setPartialResultsMode(String partialResultsMode) {
        this.partialResultsMode = partialResultsMode;
    }

    public String getPartialResultsMode() {
        return this.partialResultsMode;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    public void setResultSetCacheMode(String resultSetCacheMode) {
        this.resultSetCacheMode = resultSetCacheMode;
    }

    public String getResultSetCacheMode() {
        return this.resultSetCacheMode;
    }

    public String getShowPlan() {
        return showPlan;
    }

    public void setShowPlan(String showPlan) {
        this.showPlan = showPlan;
    }

    public void setNoExec(boolean noExec) {
        this.noExec = noExec;
    }

    public boolean isNoExec() {
        return noExec;
    }

    /**
     * Returns the current setting for how connections are created by this DataSource manage transactions
     * for client requests when client applications do not use transactions.
     * Because a virtual database will likely deal with multiple underlying information sources,
     * Teiid will execute all client requests within the contexts of transactions.
     * This method determines the semantics of creating such transactions when the client does not
     * explicitly do so.
     * @return the current setting, or null if the property has not been set and the default mode will
     * be used.
     */
    public String getAutoCommitTxn() {
        return this.transactionAutoWrap;
    }

    /**
     * Sets the setting for how connections are created by this DataSource manage transactions
     * for client requests with autoCommit = true.
     * Because a virtual database will likely deal with multiple underlying information sources,
     * Teiid will execute all client requests within the contexts of transactions.
     * This method determines the semantics of creating such transactions when the client does not
     * explicitly do so.
     * <p>
     * The allowable values for this property are:
     * <ul>
     *   <li>"<code>OFF</code>" - Nothing is ever wrapped in a transaction and the server will execute
     * multi-source updates happily but outside a transaction.  This is least safe but highest performance.
     * The {@link #TXN_WRAP_OFF} constant value is provided for convenience.</li>
     *   <li>"<code>ON</code>" - Always wrap every command in a transaction.  This is most safe but lowest
     * performance.
     * The {@link #TXN_WRAP_ON} constant value is provided for convenience.</li>
     *   <li>"<code>AUTO</code>" - checks if a command requires a transaction and will be automatically wrap it.
     * This is the default mode.
     * The {@link #TXN_WRAP_AUTO} constant value is provided for convenience.</li>
     * </ul>
     * @param transactionAutoWrap The transactionAutoWrap to set
     */
    public void setAutoCommitTxn(String transactionAutoWrap) {
        this.transactionAutoWrap = transactionAutoWrap;
    }


    public boolean getUseJDBC4ColumnNameAndLabelSemantics() {
        return useJDBC4ColumnNameAndLabelSemantics;
    }

    public void setUseJDBC4ColumnNameAndLabelSemantics(boolean useJDBC4ColumnNameAndLabelSemantics) {
        this.useJDBC4ColumnNameAndLabelSemantics = useJDBC4ColumnNameAndLabelSemantics;
    }

    // --------------------------------------------------------------------------------------------
    //                  V A L I D A T I O N   M E T H O D S
    // --------------------------------------------------------------------------------------------

    /**
     * Return the reason why the supplied application name may be invalid, or null
     * if it is considered valid.
     * @param applicationName a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setApplicationName(String)
     */
    public static String reasonWhyInvalidApplicationName( final String applicationName ) {
        return null;        // anything is valid
    }



    /**
     * Return the reason why the supplied virtual database name may be invalid, or null
     * if it is considered valid.
     * @param databaseName a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setDatabaseName(String)
     */
    public static String reasonWhyInvalidDatabaseName( final String databaseName ) {
        if ( databaseName == null || databaseName.trim().length() == 0 ) {
            return JDBCPlugin.Util.getString("MMDataSource.Virtual_database_name_must_be_specified"); //$NON-NLS-1$
        }
        return null;
    }

    /**
     * Return the reason why the supplied user name may be invalid, or null
     * if it is considered valid.
     * @param userName a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setUser(String)
     */
    public static String reasonWhyInvalidUser( final String userName ) {
        return null;
    }

    /**
     * Return the reason why the supplied transaction auto wrap value may be invalid, or null
     * if it is considered valid.
     * <p>
     * This method checks to see that the value is one of the allowable values.
     *
     * @param autoWrap a possible value for the auto wrap property.
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setAutoCommitTxn(String)
     */
    public static String reasonWhyInvalidTransactionAutoWrap( final String autoWrap ) {
        if ( autoWrap == null || autoWrap.trim().length() == 0 ) {
            return null;    // no longer require an app server name, 'cause will look on classpath
        }
        final String trimmedAutoWrap = autoWrap.trim();
        if( TXN_WRAP_ON.equals(trimmedAutoWrap) ) {
            return null;
        }
        if( TXN_WRAP_OFF.equals(trimmedAutoWrap) ) {
            return null;
        }
        if( TXN_WRAP_AUTO.equals(trimmedAutoWrap) ) {
            return null;
        }

        Object[] params = new Object[] {
            TXN_WRAP_ON, TXN_WRAP_OFF, TXN_WRAP_AUTO };
        return JDBCPlugin.Util.getString("MMDataSource.Invalid_trans_auto_wrap_mode", params); //$NON-NLS-1$
    }

    /**
     * Return the reason why the supplied virtual database version may be invalid, or null
     * if it is considered valid.
     * @param databaseVersion a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setDatabaseVersion(String)
     */
    public static String reasonWhyInvalidDatabaseVersion( final String databaseVersion ) {
        return null;        // anything is valid (let server validate)
    }

    /**
     * Return the reason why the supplied data source name may be invalid, or null
     * if it is considered valid.
     * @param dataSourceName a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setDataSourceName(String)
     */
    public static String reasonWhyInvalidDataSourceName( final String dataSourceName) {
        return null;        // anything is valid
    }

    /**
     * Return the reason why the supplied password may be invalid, or null
     * if it is considered valid.
     * @param pwd a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setPassword(String)
     */
    public static String reasonWhyInvalidPassword( final String pwd ) {
        return null;
    }

    /**
     * Return the reason why the supplied description may be invalid, or null
     * if it is considered valid.
     * @param description a possible value for the property
     * @return the reason why the property is invalid, or null if it is considered valid
     * @see #setDescription(String)
     */
    public static String reasonWhyInvalidDescription( final String description ) {
        return null;        // anything is valid
    }

    /**
     * The reason why partialResultsMode is invalid.
     * @param partialMode boolean flag
     * @return String reason
     */
    public static String reasonWhyInvalidPartialResultsMode( final String partialMode) {
        if ( partialMode != null ) {
            if (partialMode.equalsIgnoreCase("true") || partialMode.equalsIgnoreCase("false")) { //$NON-NLS-1$ //$NON-NLS-2$
                return null;
            }
            return JDBCPlugin.Util.getString("MMDataSource.The_partial_mode_must_be_boolean._47"); //$NON-NLS-1$
        }
        return null;
    }

    /**
     * The reason why fetchSize is invalid.
     * @param fetchSize Number of rows per batch
     * @return the reason why the property is invalid, or null if it is considered valid
     */
    public static String reasonWhyInvalidFetchSize( final int fetchSize) {
        if ( fetchSize <= 0 ) {
            return JDBCPlugin.Util.getString("MMDataSource.The_fetch_size_must_be_greater_than_zero"); //$NON-NLS-1$
        }
        return null;
    }

    public void setAdditionalProperties(String additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public String getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAnsiQuotedIdentifiers(boolean ansiQuotedIdentifiers) {
        this.ansiQuotedIdentifiers = ansiQuotedIdentifiers;
    }

    public boolean isAnsiQuotedIdentifiers() {
        return ansiQuotedIdentifiers;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

}

